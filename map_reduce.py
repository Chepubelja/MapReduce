"""
    MapReduce Framework (with map, combine, shuffle and reduce).

    Examples of usage:

    python map_reduce.py data/test.txt
    python map_reduce.py data/test.txt --num_mappers 10
    python map_reduce.py data/test.txt --num_mappers 10 --num_reducers 10
    python map_reduce.py data/test.txt --num_mappers 10 --num_reducers 10 --use_combiners

"""
import argparse
import csv
import string
import time
import threading


class MapReduce(object):
    """
    Map Reduce Class.
    """

    def __init__(self, filename, num_mappers, num_reducers, use_combiners):
        """
        Init.
        """
        self.filename = filename
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.use_combiners = use_combiners
        self.data = ["" for x in range(num_mappers)]
        self.map_result, self.combine_result, self.reduce_result = [], [], []
        self.reader = self.create_reader()
        self.reader.join()
        self.mappers = self.create_mapper()
        print("Map", self.map_result)
        if use_combiners:
            self.combiners = self.create_combiner()
            self.join_threads(self.mappers)
            self.join_threads(self.combiners)
            print("Combine result", self.combine_result)
            self.shuffle_input = self.combine_result
        else:
            self.join_threads(self.mappers)
            self.shuffle_input = self.map_result
        self.shuffle_result = self.shuffle()

        self.reducers = self.create_reducer()
        self.join_threads(self.reducers)

    def mapper(self):
        """
        Function that adds every result of map_line() to self.map_result.
        """
        while len(self.data) != 0:
            self.map_result.append(self.map_line(self.data.pop(0)))

    @staticmethod
    def map_line(line):
        """
        Function that maps every word in the sequence to the tuple (word, 1).
        :param line: string with words.
        :return: array of tuples (word, 1).
        """
        return [(word, 1) for word in line.split()]

    def combiner(self):
        """
        Function that adds every result of combine() to self.combine_result.
        """
        while len(self.map_result) != 0:
            self.combine_result.append(self.combine(self.map_result.pop(0)))

    @staticmethod
    def combine(line):
        """
        Function that count every occurrence of word in the sequence.
        :return: array of tuples (word, number_of_occurrences)
        """
        combine_result = {}
        for pair in line:
            if pair[0] in combine_result.keys():
                combine_result[pair[0]] += pair[1]
            else:
                combine_result[pair[0]] = pair[1]
        return combine_result

    def shuffle(self):
        """
        Function that shuffles each tuple [(word_1, occurrences_1), (word_1, occurrences_2)]
        to view [(word_1, [ occurrences_1, occurrences_2] )]
        """
        shuffle_result = []
        temp_dict = {}
        temp_lst = [(key, value) for d in self.shuffle_input for key, value in d.items()]
        for pair in temp_lst:
            if pair[0] in temp_dict.keys():
                temp_dict[pair[0]].append(pair[1])
            else:
                temp_dict[pair[0]] = [pair[1]]
        [shuffle_result.append((key, value)) for key, value in temp_dict.items()]
        return shuffle_result

    def reducer(self):
        """
        Function that adds every result of reduce() to self.reduce_result.
        """
        while len(self.shuffle_result) != 0:
            self.reduce_result.append(self.reduce(self.shuffle_result.pop(0)))

    @staticmethod
    def reduce(pair):
        """
        Function that sums up all occurrences of word.
        :return: tuple (word, all_occurrences)
        """
        return pair[0], sum(pair[1])

    def read_file_txt(self):
        """
        Function that read txt file and divide all text on number of mappers.
        """
        with open(self.filename) as file:
            temp_data = [self.remove_punctuation(line) for line in file
                         if self.remove_punctuation(line).split()]
        start_idx = 0
        for i, line in enumerate(temp_data):
            self.data[start_idx] += line.rstrip() + " "
            if (i + 1) % (len(temp_data) // self.num_mappers) == 0:
                start_idx += 1
                if start_idx == len(self.data):
                    start_idx = 0

    def read_file_csv(self):
        """
        Function that read csv file and divide all text on number of mappers.
        """
        with open(self.filename) as file:
            reader = csv.reader(file)
            temp_data = [self.remove_punctuation(' '.join(line)) for line in reader
                         if self.remove_punctuation(line) and line]
        start_idx = 0
        for i, line in enumerate(temp_data):
            self.data[start_idx] += line.rstrip() + " "
            if (i + 1) % (len(temp_data) // self.num_mappers) == 0:
                start_idx += 1
                if start_idx == len(self.data):
                    start_idx = 0

    def create_mapper(self):
        threads = []
        while len(threads) == 0:
            if len(self.data) != 0:
                for i in range(self.num_mappers):
                    t = threading.Thread(target=self.mapper)
                    t.start()
                    threads.append(t)
        return threads

    def create_reader(self):
        if self.filename.endswith('.txt'):
            t = threading.Thread(target=self.read_file_txt)
            t.start()
            return t
        elif self.filename.endswith('.csv'):
            t = threading.Thread(target=self.read_file_csv)
            t.start()
            return t

    def create_combiner(self):
        threads = []
        while len(threads) == 0:
            if len(self.map_result) != 0:
                for i in range(self.num_mappers):
                    t = threading.Thread(target=self.combiner)
                    t.start()
                    threads.append(t)
        return threads

    def create_reducer(self):
        threads = []
        while len(threads) == 0:
            if len(self.shuffle_result) != 0:
                for i in range(self.num_reducers):
                    t = threading.Thread(target=self.reducer)
                    t.start()
                    threads.append(t)
        return threads

    @staticmethod
    def join_threads(threads):
        """
        Function that joins multiple threads.
        """
        for thread in threads:
            thread.join()

    @staticmethod
    def remove_punctuation(input_str):
        """
        Removes all punctuation from string.
        :param input_str: Input string.
        :return: String without any punctuation.
        """
        return ''.join(ch for ch in input_str if ch not in set(string.punctuation)).lower() + ' '


def main():
    """
    Main function.
    """
    parser = argparse.ArgumentParser(description='Process configuration arguments')
    required_args = parser.add_argument_group('REQUIRED arguments')
    required_args.add_argument(action='store', dest='path_to_file',
                               type=str, help="Enter path/to/input/file (txt or csv)")

    optional_args = parser.add_argument_group('OPTIONAL arguments')
    optional_args.add_argument('--num_mappers', action='store', dest='num_mappers',
                               type=int, default=4, help="Enter number of mappers: 1, 3, 6...")
    optional_args.add_argument('--num_reducers', action='store', dest='num_reducers',
                               type=int, default=4, help="Enter number of reducers: 1, 3, 6...")
    optional_args.add_argument('--use_combiners', action='store_true', dest='use_combiners',
                               default=False, help="Add flag if you want to use combiners")
    args = parser.parse_args()
    print("===============================================")
    print("Path to file:   ", args.path_to_file)
    print("Num of mappers: ", args.num_mappers)
    print("Num of reducers:", args.num_reducers)
    print("Use combiners:  ", args.use_combiners)
    print("===============================================")

    start_time = time.time()
    MapReduce(args.path_to_file, num_mappers=args.num_mappers,
              num_reducers=args.num_reducers, use_combiners=args.use_combiners)
    print("Time of execution: ", time.time() - start_time)


if __name__ == "__main__":
    main()

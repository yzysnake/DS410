from mrjob.job import MRJob  # MRJob version
import string

string.punctuation += '”“'


class WordCount(MRJob):  # MRJob version
    def mapper_init(self):
        self.cache = {}

    def mapper(self, key, line):
        line = line.translate(str.maketrans('', '', string.punctuation)).strip('\n')  ## get rid of punctuation
        words = line.split(" ")
        for w in words:
            if w.isalpha():
                w = w.lower()
                if self.cache.get(w) is None:
                    self.cache.update({w: 1})
                else:
                    self.cache[w] += 1
            if len(self.cache) > 99:
                for i in self.cache:
                    yield i, self.cache[i]
                self.set_status('Alive!!!')
                self.cache.clear()

    def mapper_final(self):
        if len(self.cache) > 0:
            for i in self.cache:
                yield i, self.cache[i]
            self.cache.clear()

    def reducer_init(self):
        self.cache = {}

    def reducer(self, key, values):
        self.cache.update({key: sum(values)})
        yield key, sum(values)
        if len(self.cache) > 99:
            sorted_tuples = sorted(self.cache.items(), key=lambda x: x[1])
            sorted_dict = dict(sorted_tuples)
            self.cache.clear()
            self.cache.update({list(sorted_dict.items())[-1]: sorted_dict[list(sorted_dict.items())[-1]]})
            self.set_status("Alive!!!")

    def reducer_final(self):
        if len(self.cache) > 0:
            sorted_tuples = sorted(self.cache.items(), key=lambda x: x[1])
            sorted_dict = dict(sorted_tuples)
            yield 'MostFrequent', list(sorted_dict.items())[-1]


if __name__ == '__main__':
    WordCount.run()  # MRJob version

from mrjob.job import MRJob  # MRJob version
from mrjob.step import MRStep

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
        x = sum(values)
        self.cache.update({key: x})
        yield key, x

    def reducer_final(self):
        if len(self.cache) > 0:
            yield 'MostFrequent', max(self.cache, key=self.cache.get)


if __name__ == '__main__':
    WordCount.run()  # MRJob version

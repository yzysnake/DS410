from mrjob.job import MRJob   # MRJob version

# Change the class name!!
class WordCount(MRJob):  #MRJob version
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            if len(w) <= 2:
                pass
            else:
                yield (len(w), 1)

    def reducer(self, key, values):
        # values is an iterator, you can only read it once
        num = sum(values)
        if num <= 1:
            pass
        else:
            yield (key, num)

if __name__ == '__main__':
    WordCount.run()   # MRJob version

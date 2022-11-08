from mrjob.job import MRJob  # MRJob version


# Change the class name!!
class PriceCount(MRJob):  # MRJob version
    def mapper(self, _, line):
        words = line.split(',')
        if words[3] == 'ORIGIN' or words[4] == 'ORIGIN_STATE_NM' or words[7] == 'PASSENGERS':
            pass
        else:
            ORIGIN = str(words[3])
            ORIGIN_STATE_NM = str(words[4])
            PASSENGERS = float(words[7])
            yield (ORIGIN, PASSENGERS)
            yield (ORIGIN_STATE_NM, PASSENGERS)

    def reducer(self, key, values):

        yield (key, sum(values))


if __name__ == '__main__':
    PriceCount.run()  # MRJob version
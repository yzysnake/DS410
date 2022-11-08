
from mrjob.job import MRJob  # MRJob version


# Change the class name!!
class PriceCount(MRJob):  # MRJob version
    def mapper(self, _, line):
        words = line.split(',')
        if words[1] == 'YEAR':
            pass
        else:
            year = int(words[1])
            ORIGIN = str(words[3])
            PASSENGERS = float(words[7])
            yield (ORIGIN, (year, PASSENGERS))

    def reducer(self, key, values):
        outgoing_sum_2021 = 0
        outgoing_sum_2022 = 0
        total = 0
        difference = 0
        for t in values:
            if t[0] == 2021:
                outgoing_sum_2021 += t[1]
            else:
                outgoing_sum_2022 += t[1]
            total = outgoing_sum_2021 + outgoing_sum_2022
            difference = outgoing_sum_2022 - outgoing_sum_2021
        yield (key, (total, difference))


if __name__ == '__main__':
    PriceCount.run()  # MRJob version

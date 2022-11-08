from mrjob.job import MRJob  # MRJob version


# Change the class name!!
class PriceCount(MRJob):  # MRJob version
    def mapper(self, _, line):
        words = line.split(',')
        if words[4] == 'ORIGIN_STATE_NM' or words[6] == 'DEST_STATE_NM' or words[7] == 'PASSENGERS':
            pass
        else:
            ORIGIN_STATE_NM = str(words[4])
            DEST_STATE_NM = str(words[6])
            PASSENGERS = float(words[7])
            yield (ORIGIN_STATE_NM, (0, PASSENGERS))
            yield (DEST_STATE_NM, (PASSENGERS, 0))

    def reducer(self, key, values):
        incoming_sum = 0
        outgoing_sum = 0
        for t in values:
            incoming_sum += t[0]
            outgoing_sum += t[1]
        yield (key, (incoming_sum,outgoing_sum))


if __name__ == '__main__':
    PriceCount.run()  # MRJob version

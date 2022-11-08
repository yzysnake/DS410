from mrjob.job import MRJob  # MRJob version


# Change the class name!!
class EfficientFlights(MRJob):  # MRJob version
    def mapper_init(self):
        self.cache = {}


    def mapper(self, _, line):
        words = line.split(',')
        if words[4] == 'ORIGIN_STATE_NM' or words[6] == 'DEST_STATE_NM' or words[7] == 'PASSENGERS':
            pass
        else:
                ORIGIN = str(words[3])
                DEST = str(words[5])
                PASSENGERS = float(words[7])
                if self.cache.get(ORIGIN) is None:
                    self.cache.update({ORIGIN: [0, PASSENGERS]})
                else:
                    self.cache[ORIGIN][1] += PASSENGERS
                if self.cache.get(DEST) is None:
                    self.cache.update({DEST: [PASSENGERS, 0]})
                else:
                    self.cache[DEST][0] += PASSENGERS
                if len(self.cache) > 99:
                    for i in self.cache:
                        yield i, self.cache[i]
                    self.set_status("Alive!!!")
                    self.cache.clear()

    def mapper_final(self):
        if len(self.cache) > 0:
            for i in self.cache:
                yield i, self.cache[i]
            self.cache.clear()

    def reducer(self, key, values):
        incoming_sum = 0
        outgoing_sum = 0
        for t in values:
            incoming_sum += t[0]
            outgoing_sum += t[1]
        yield (key, ("arrive: " + str(incoming_sum), "leave: " + str(outgoing_sum)))


if __name__ == '__main__':
    EfficientFlights.run()  # MRJob version

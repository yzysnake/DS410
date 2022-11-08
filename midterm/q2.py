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
            year = int(words[1])
            ORIGIN = str(words[3])
            PASSENGERS = float(words[7])

            if self.cache.get(ORIGIN) is None:

                self.cache.update({ORIGIN: [[2021, 0], [2022, 0]]})
                if year == 2021:
                    self.cache[ORIGIN][0][1] += PASSENGERS
                else:
                    self.cache[ORIGIN][1][1] += PASSENGERS
            else:

                if year == 2021:
                    self.cache[ORIGIN][0][1] += PASSENGERS
                else:
                    self.cache[ORIGIN][1][1] += PASSENGERS

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
        outgoing_sum_2021 = 0
        outgoing_sum_2022 = 0
        total = 0
        difference = 0
        for i in values:
            outgoing_sum_2021 += i[0][1]
            outgoing_sum_2022 += i[1][1]
        total = outgoing_sum_2021 + outgoing_sum_2022
        difference = outgoing_sum_2022 - outgoing_sum_2021
        yield (key, (total, difference))


if __name__ == "__main__":
    EfficientFlights.run()

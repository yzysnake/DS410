from mrjob.job import MRJob  # MRJob version


# Change the class name!!
class EfficientFlights(MRJob):  # MRJob version
    def mapper_init(self):
        self.cache = {}


    def mapper(self, _, line):
        words = line.split(',')
        if words[0] == "ORIGIN":
            pass
        else:
                ORIGIN = str(words[0])
                ORIGIN_STATE = str(words[1])
                PASSENGERS = float(words[2])
                if self.cache.get(ORIGIN_STATE) is None:
                    self.cache.update({ORIGIN_STATE: [[ORIGIN],PASSENGERS]})
                else:
                    self.cache[ORIGIN_STATE][1] += PASSENGERS
                    self.cache[ORIGIN_STATE][0].append(ORIGIN)
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
        sum_amount = 0
        ORIGIN_list = []
        for t in values:
            sum_amount += t[1]
            for i in t[0]:
                ORIGIN_list.append(i)
        ORIGIN_list = [*set(ORIGIN_list)]
        num_ORIGIN = len(ORIGIN_list)
        yield (key, (ORIGIN_list,sum_amount))


if __name__ == '__main__':
    EfficientFlights.run()  # MRJob version

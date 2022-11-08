
from mrjob.job import MRJob  # MRJob version


# Change the class name!!
class PassengerCount(MRJob):  # MRJob version
    
    def mapper_init(self):
        self.cache = {}
        
    def mapper(self, _, line):
        words = line.split(',')
        if words[1] == 'YEAR':
            pass
        else:
            year = int(words[1])
            ORIGIN = str(words[3])
            ORIGIN_STATE = str(words[4])
            DEST_STATE = str(words[6])
            PASSENGERS = float(words[7])
            temp = ORIGIN + '_' + DEST_STATE
            if self.cache.get(temp) is None:
                self.cache.update({temp: [[2021, 0], [2022, 0]]})
                if year == 2021:
                    self.cache[temp][0][1] += PASSENGERS
                else:
                    self.cache[temp][1][1] += PASSENGERS
            else:

                if year == 2021:
                    self.cache[temp][0][1] += PASSENGERS
                else:
                    self.cache[temp][1][1] += PASSENGERS
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
        for i in values:
            outgoing_sum_2021 += i[0][1]
            outgoing_sum_2022 += i[1][1]
        ORIGIN = str(key).split('_')[0]
        DEST = str(key).split('_')[1]
        
        yield (ORIGIN, (DEST,outgoing_sum_2021, outgoing_sum_2022))

       
if __name__ == '__main__':
    PassengerCount.run()  # MRJob version

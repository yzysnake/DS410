from mrjob.job import MRJob  # MRJob version


# Change the class name!!
class PassengerCount(MRJob):  # MRJob version
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
            if year == 2021:
                yield (temp, [[2021, PASSENGERS], [2022, 0]])
            else:
                yield (temp, [[2021, 0], [2022, PASSENGERS]])
            
            

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

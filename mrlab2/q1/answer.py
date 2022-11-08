from mrjob.job import MRJob  # MRJob version

# Change the class name!!


COUNTER_GROUP = 'MyErrors'
COUNTER = "ConversionError"

class PriceCount(MRJob):  # MRJob version
    def mapper(self, _, line):
        words = line.split(',')
        try:
            Quantity = float(words[3])
            UnitPrice = float(words[5])
            self.set_status("Alive")
        except:
            yield (words[0], "|".join(words))


    def reducer(self, key, values):
        yield (key, "!".join(values))


if __name__ == '__main__':
    PriceCount.run()  # MRJob version
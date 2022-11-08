from mrjob.job import MRJob  # MRJob version


class EfficientItems(MRJob):  # MRJob version
    def mapper_init(self):
        self.cache = {}

    def mapper(self, key, line):
        parts = line.split("\t")
        if "Invoice" not in parts[0]:
            stockcode = str(parts[1])
            amount = float(parts[3])
            country = str(parts[7])
            if self.cache.get(stockcode) is None:
                self.cache.update({stockcode: [amount, [country]]})
            else:
                self.cache[stockcode][0] += amount
                self.cache[stockcode][1].append(country)
            if len(self.cache) > 99:
                for i in self.cache:
                    yield (i, (self.cache[i][0], self.cache[i][1]))
                self.set_status('Alive!!!')
                self.cache.clear()

    def mapper_final(self):
        if len(self.cache) > 0:
            for i in self.cache:
                yield (i, [self.cache[i][0], self.cache[i][1]])
            self.cache.clear()

    def reducer(self, key, values):
        sum_amount = 0
        country = []
        for i in values:
            sum_amount += i[0]
            for b in i[1]:
                country.append(b)
        most_country_frequency = max(set(country), key=country.count)
        country = [*set(country)]
        num_countries = len(country)

        yield (key, ["amount: " + str(sum_amount), "numcountries: " + str(num_countries),
                     "mostpopular: " + str(most_country_frequency)])


if __name__ == '__main__':
    EfficientItems.run()  # MRJob version

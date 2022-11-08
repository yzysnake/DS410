from mrjob.job import MRJob  # MRJob version


class Items(MRJob):  # MRJob version
    def mapper(self, key, line):
        parts = line.split("\t")
        if "Invoice" not in parts[0]:
            stockcode = str(parts[1])
            amount = float(parts[3])
            country = parts[7]
            yield (stockcode, (amount, country))

    def reducer(self, key, values):
        sum_amount = 0
        country = []
        for i in values:
            sum_amount += i[0]
            country.append(i[1])
        most_country_frequency = max(set(country), key=country.count)
        country = [*set(country)]
        num_countries = len(country)

        yield (key, ["amount: " + str(sum_amount), "numcountries: " + str(num_countries),
                     "mostpopular: " + str(most_country_frequency)])


if __name__ == '__main__':
    Items.run()  # MRJob version

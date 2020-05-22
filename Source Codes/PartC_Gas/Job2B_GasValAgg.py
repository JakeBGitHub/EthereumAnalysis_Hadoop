from mrjob.job import MRJob
import re
import time
import datetime

#This line declares the class PartBJob1, that extends the MRJob format.
class GasTimeSort(MRJob):

# this class will define two additional methods: the mapper method goes here:
    def mapper(self, _, line):
        try:
            fields = line.split("\t")
            if len(fields) == 2:
                fields2 = fields[1].split(',')
                date = fields2[0][2:9]
                gas = fields2[1].replace('"', '')
                value = fields2[2][2:].replace('"]', '')

                yield (date, (gas, value, 1))
        except:
            pass

    def combiner(self, key, sum):
        value_total = 0
        gas_total = 0
        count_total = 0
        for n in sum:
            gas_total += int(n[0])
            value_total += int(n[1])
            count_total += int(n[2])
        yield (key, (gas_total, value_total, count_total))

#and the reducer method goes after this line:
    def reducer(self, key, sum):
        value_total = 0
        gas_total = 0
        count_total = 0
        for n in sum:
            gas_total += int(n[0])
            value_total += int(n[1])
            count_total += int(n[2])
        yield (key, (gas_total / count_total, value_total / count_total))


if __name__ == '__main__':
    GasTimeSort.run()

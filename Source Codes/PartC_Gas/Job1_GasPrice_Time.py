from mrjob.job import MRJob
import re
import time
import datetime

#This line declares the class PartBJob1, that extends the MRJob format.
class GasPrice_Time(MRJob):

# this class will define two additional methods: the mapper method goes here:
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                time_epoch = int(fields[6])
                month = time.strftime("%m",time.gmtime(time_epoch)) #returns each month
                year = time.strftime("%Y",time.gmtime(time_epoch)) #returns each year
                key = (year, month)
                gas_price = int(fields[5])
                yield (key, (gas_price, 1))
        except:
            pass

    def combiner(self, key, gas_price_sum):
        gas_price_total = 0
        count_total = 0
        for n in gas_price_sum:
            gas_price_total += int(n[0])
            count_total += int(n[1])
        yield (key, (gas_price_total, count_total))

#and the reducer method goes after this line:
    def reducer(self, key, gas_price_sum):
        gas_price_total = 0
        count_total = 0
        for n in gas_price_sum:
            gas_price_total += int(n[0])
            count_total += int(n[1])
        yield (key, gas_price_total / count_total)

#

if __name__ == '__main__':
    GasPrice_Time.run()

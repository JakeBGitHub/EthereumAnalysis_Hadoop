"""PartA. Transaction Count for every month between the start and end of the dataset
"""
from mrjob.job import MRJob
import re
import time
import datetime
# import numpy as np


#This line declares the class PartA, that extends the MRJob format.
class PartA(MRJob):

# this class will define two additional methods: the mapper method goes here:
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                time_epoch = int(fields[6])
                month = time.strftime("%m",time.gmtime(time_epoch)) #returns each month
                year = time.strftime("%Y",time.gmtime(time_epoch)) #returns each year
                key = (year, month)
                yield(key, 1)
        except:
            pass

#and the reducer method goes after this line:
    def reducer(self, key, trans_count):
        yield(key, sum(trans_count))

#this part of the python script tells to actually run the defined MapReduce job. Note that PartA is the name of the class
if __name__ == '__main__':
    PartA.run()

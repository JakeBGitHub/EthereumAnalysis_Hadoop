"""PartBJob1. Initial Aggregation
"""
from mrjob.job import MRJob
import re
import time
import datetime

#This line declares the class PartBJob1, that extends the MRJob format.
class PartBJob1(MRJob):

# this class will define two additional methods: the mapper method goes here:
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                to_addr = fields[2]
                value = int(fields[3])
                yield(to_addr, value)
        except:
            pass

#and the reducer method goes after this line:
    def reducer(self, to_addr, value):
        yield (to_addr, sum(value))

#this part of the python script tells to actually run the defined MapReduce job. Note that PartBJob1 is the name of the class
if __name__ == '__main__':
    PartBJob1.run()

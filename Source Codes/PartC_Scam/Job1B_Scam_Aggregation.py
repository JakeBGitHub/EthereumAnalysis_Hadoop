from mrjob.job import MRJob
import re
import time
import datetime

#This line declares the class PartBJob1, that extends the MRJob format.
class scam_agg(MRJob):

# this class will define two additional methods: the mapper method goes here:
    def mapper(self, _, line):
        try:
            fields = line.split("\t")
            if len(fields) == 2:
                category = fields[0]
                value = int(fields[1])
                yield (category, value)
        except:
            pass

#and the reducer method goes after this line:
    def reducer(self, category, value):
        yield (category, sum(value))

#this part of the python script tells to actually run the defined MapReduce job. Note that PartBJob1 is the name of the class
if __name__ == '__main__':
    scam_agg.run()

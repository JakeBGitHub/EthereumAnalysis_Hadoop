"""PartBJob3. Top Ten
"""
from mrjob.job import MRJob

#This line declares the class PartBJob3, that extends the MRJob format.
class PartBJob3(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        try:
            if len(line.split('\t')) == 2:
                line = line.strip()
                fields = line.split('\t')
                address = fields[0]
                val = int(fields[1])
                yield (None, (address, val))
        except:
            pass


    def combiner(self, _, unsorted_values):
        sorted_values = sorted(unsorted_values, reverse=True, key = lambda unsorted_vals:unsorted_vals[1])
        i=0
        for value in sorted_values:
            yield("top", value)
            i += 1
            if i >= 10:
                break

#and the reducer method goes after this line

    def reducer(self, _, unsorted_values):
        sorted_values = sorted(unsorted_values, reverse=True, key = lambda tup:tup[1])
        i=0
        for value in sorted_values:
            yield("{} - {}".format(value[0], value[1]), None)
            i += 1
            if i >= 10:
                break


#this part of the python script tells to actually run the defined MapReduce job. Note that PartBJob3 is the name of the class
if __name__ == '__main__':
    PartBJob3.run()

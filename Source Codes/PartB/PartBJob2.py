"""PartBJob2. Joining Transactions/Contracts and Filtering
"""

from mrjob.job import MRJob

class PartBJob2(MRJob):

    def mapper(self, _, line):
        try:
            #one mapper, we need to first differentiate among both types
            if len(line.split(','))==5:
                # line = line.strip()
                fields = line.split(',')
                join_key = fields[0]
                join_value = int(fields[3])
                # print(join_key)
                yield (join_key, (join_value,1)) # 1 for CONTRACTS


            elif len(line.split('\t'))==2:
                # line = line.strip()
                fields = line.split('\t')
                join_key = fields[0]
                join_value = int(fields[1])
                join_key = join_key.replace('"', '')
                yield (join_key,(join_value,2)) # 2 for JOB1 OUTPUT
        except:
            pass

    def reducer(self, address, values):

        val = ['','']

        for value in values:
            if value[1] == 1:
                val[0] = value[0]
            elif value[1] == 2:
                val[1] = value[0]

        if val[0] != '':
            yield (address, val[1])


if __name__ == '__main__':
    PartBJob2.run()

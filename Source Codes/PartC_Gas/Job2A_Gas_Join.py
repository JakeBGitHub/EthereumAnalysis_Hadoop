from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time
import datetime

class gas_join(MRJob):


    contracts = {}

    def mapper_join_init(self):
        # load companylist into a dictionary
        # run the job with --file input/companylist.tsv
        with open("PartBTopTen.tsv") as f:
            for line in f:
                fields = line.split("\t")
                key = fields[0]
                val = fields[1]
                self.contracts[key] = val

    def mapper_repl_join(self, _, line):
        fields = line.split(",")


        for i in self.contracts:
            to_address = fields[2]

            if to_address == i:
                value = fields[3]
                gas = fields[4]
                time_epoch = int(fields[6])
                time_t = time.strftime("%Y-%m", time.gmtime(time_epoch))
                tup = (time_t, gas, value)
                yield (to_address, tup)
                break
            else:
                pass

    def mapper_length(self, key, values):
        yield(key, list(values))

    def reducer_sum(self, key, values):
        for val in values:
            yield (key, (val[0], val[1], val[2]))

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_repl_join),
                MRStep(mapper=self.mapper_length,
                        reducer=self.reducer_sum)]


if __name__ == '__main__':
    gas_join.run()

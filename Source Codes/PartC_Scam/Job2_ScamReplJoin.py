from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time
import datetime
import json
import io

class scam_join2(MRJob):

    scam = {}

    def mapper_join_init(self):
        json_file = io.open("scams.json", 'r', encoding='utf-8-sig').read()
        p_json = json.loads(json_file)
        result = p_json['result']
        for r in result:
            self.scam[r] = result[r]['category']

    def mapper_repl_join(self, _, line):
        fields = line.split(",")
        for i in self.scam:
            to_address = fields[2]

            if to_address == i:
                value = int(fields[3])
                time_epoch = fields[6]
                date = time.strftime("%Y-%m", time.gmtime(int(time_epoch)))
                yield ((date, self.scam[i]), value)
                break
            else:
                pass

    def mapper_length(self, key, values):
        yield(key, values)

    def reducer_sum(self, key, values):
            yield (key, sum(values))

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_repl_join),
                MRStep(mapper=self.mapper_length,
                        reducer=self.reducer_sum)]



if __name__ == '__main__':
    scam_join2.run()

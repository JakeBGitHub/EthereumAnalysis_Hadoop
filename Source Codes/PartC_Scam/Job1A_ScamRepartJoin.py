from mrjob.job import MRJob

class scam_join(MRJob):

    def mapper(self, _, line):
        try:
            if len(line.split(','))==37:
                line = line.strip()
                fields = line.split(',')
                join_key = fields[1]      #join_key is scam_address
                join_key = join_key.replace('"', '')
                join_value = fields[6]      # join_value is category
                yield (join_key, (join_value,1)) # 1 for scams

            elif len(line.split('\t'))==2:
                line = line.strip()
                fields = line.split('\t')
                join_key = fields[0]                #join key is to_address
                join_value = int(fields[1])       #join_value is   value sums
                join_key = join_key.replace('"', '')
                yield (join_key,(join_value,2)) # 2 for JOB1 OUTPUT

        except:
            pass

    def reducer(self, address, values):
        category = ''
        val = ''

        for value in values:
            if value[1] == 1:
                category = value[0]
            elif value[1] == 2:
                val = value[0]

        if category != '' and val !='':
            yield (category, val)


if __name__ == '__main__':
    scam_join.run()

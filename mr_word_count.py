from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_friends,
                   reducer=self.reducer_count_friends),
            MRStep(mapper=self.mapper_socialrank,
                   reducer=self.reducer_socialrank)
        ]

    def mapper_get_friends(self, _, line):
        words = line.split()
        yield (words[0], words[1])

    def reducer_count_friends(self, id, friends):
        frnd = []
        for x in friends:
            frnd.append(x)
        yield (id, (frnd, 1))

    def mapper_socialrank(self, key, friends):
        rank = float(friends[1])/float(len(friends[0]))
        for x in friends[0]:
            yield (x, ([], rank))
        yield (key, (friends[0], 0))


    def reducer_socialrank(self, key, friends):
        total = 0
        frnd = []
        for x in friends:
            total += x[1]
            frnd = x[0] + frnd
        total = 0.15 + 0.85*total
        yield (key, (frnd, total))


if __name__ == '__main__':
    MRMostUsedWord.run()
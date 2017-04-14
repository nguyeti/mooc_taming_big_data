import re
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordsFrequencyCount(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_get_words, reducer=self.reducer_get_count),
                MRStep(mapper=self.mapper_get_count_as_key, reducer=self.reducer_get_count_as_key)]

    def mapper_get_words(self, key, line):
        pattern = r"[\w']+"
        regex = re.compile(pattern)
        words = regex.findall(unicode(line, errors='replace'))
        for word in words:
            yield word.lower(), 1

    def reducer_get_count(self, word, occurences):
        yield word, sum(occurences)

    def mapper_get_count_as_key(self, word, count):
        yield "%04d" % int(count), word

    def reducer_get_count_as_key(self, count, words):
        for word in words:
            yield count, word


if __name__ == "__main__":
    MRWordsFrequencyCount.run()

from mrjob.job import MRJob

class MRWordsFrequencyCount(MRJob):
    def mapper(self, key, line):
        words = unicode(line, errors='replace').split()
        for word in words:
            yield word.lower(), 1
            
    def reducer(self, word, occurences):
        yield word, sum(occurences)
        
if __name__ == "__main__":
    MRWordsFrequencyCount.run()
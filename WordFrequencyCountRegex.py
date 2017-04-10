from mrjob.job import MRJob
import re

class MRWordsFrequencyCount(MRJob):
    def mapper(self, key, line):
        pattern = r"[\w']+"
# using re.compile() and saving the resulting regular expression object for reuse is more efficient when the expression will be used several times in a single program.
        regex = re.compile(pattern)
        words = regex.findall(unicode(line, errors='replace'))
        for word in words:
            #if word.lower() in ["love","salvation","jesus"]:
            yield word.lower(), 1
            
    def reducer(self, word, occurences):
        yield word, sum(occurences)
        
if __name__ == "__main__":
    MRWordsFrequencyCount.run()
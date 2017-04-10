from mrjob.job import MRJob

# we want the count of rating by rate

class MRRatingCounter(MRJob):
    # gives 12:1 12:1 16:1 115:1 659:1
    # sort and group : 12:1,1 16:1 115:1 659:1
     
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1
        
    # sum the occurences per rating: 12:2 16:1 115:1 659:1
    def reducer(self, rating, occurences):
        yield rating, sum(occurences)
        
if __name__ == "__main__":
    MRRatingCounter.run()
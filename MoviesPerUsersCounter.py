# how many movies each users watched? 
#input data 
#     |
#11:236 16:256 11:12 (mapper)
#      |
#11:236,12 16:256 (sort and group)      
#     |
#11:2 16:1 (reducer)

from mrjob.job import MRJob

class MRMoviesPerUsersCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield userID, movieID
#        print(userID, movieID)
        
    def reducer(self, userID, movieList):
        movieCount = 0
        for i in movieList:
            movieCount += 1
            
        yield userID, movieCount

if __name__ == "__main__":
    MRMoviesPerUsersCounter.run()



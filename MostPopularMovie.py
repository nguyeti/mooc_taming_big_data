# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostPopularMovie(MRJob):
    def steps(self):
        return [ MRStep(mapper=self.mapper_get_ratings, reducer=self.reducer_get_count),
                  MRStep(reducer=self.reducer_get_most_popular)]
                  
    # movieId_1:1 movieId_2:1 0 movieId_1:1
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split("\t")
        yield movieID, int(rating)
    
    # mettre sous la forme clé=None : (rating_count:movieID)
    #
    def reducer_get_count(self, movieID, ratings):
        yield None, (sum(ratings), movieID)
        
    # get the max of the values. Max get the max of the fisrt values in (rating:movie)
    def reducer_get_most_popular(self, key, values):
        yield max(values)
        
if __name__ == "__main__":
    MRMostPopularMovie.run()
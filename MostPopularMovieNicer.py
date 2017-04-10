# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostPopularMovie(MRJob):
    # tell MRJob we have additional options we want to run               
    def configure_options(self):
        super(MRMostPopularMovie, self).configure_options()
        self.add_file_option("--items",help="Path to u.ITEM")

    def steps(self):
        return [ MRStep(mapper=self.mapper_get_ratings,
                         reducer_init=self.reducer_init,
                         reducer=self.reducer_get_count),
                  MRStep(reducer=self.reducer_get_most_popular)]
    
    
    # movieId_1:1 movieId_2:1 0 movieId_1:1
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split("\t")
        yield movieID, int(rating)
    
    def reducer_init(self):
        self.moviesNames = {}
        
        with open("u.ITEM") as moviesDetails:
            for line in moviesDetails: 
                fields = unicode(line, errors='replace').split("|")
                self.moviesNames[fields[0]] = fields[1]
                
    # mettre sous la forme clé=None : (rating_count:movieID)
    #
    def reducer_get_count(self, movieID, ratings):
        yield None, (sum(ratings), self.moviesNames[movieID])
        
    # get the max of the values. Max get the max of the fisrt values in (rating:movie)
    def reducer_get_most_popular(self, key, values):
        yield max(values)
        
if __name__ == "__main__":
    MRMostPopularMovie.run()
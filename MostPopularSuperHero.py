# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostPopularSuperHero(MRJob):
    
    def configure_options(self):
        super(MRMostPopularSuperHero, self).configure_options()
        self.add_file_option("--names", help = "path to Marvel-names.txt")
    
    def steps(self):
        return [ MRStep(mapper = self.mapper_count_friends_per_line,reducer = self.reducer_total_friend_per_hero),
        MRStep(mapper = self.mapper_sort_by_friends, mapper_init=self.mapper_load_names, reducer = self.reducer_most_friend)]
        
    def mapper_count_friends_per_line(self, _, line):
        fields = line.split()
        heroID = fields[0]
        numFriends = len(fields) - 1
        yield int(heroID), int(numFriends)           
        
    def reducer_total_friend_per_hero(self, heroID, numFriends):
        yield heroID, sum(numFriends)
    # Lancer avant le mapper_sort_by_friend avec le mapper_init
    # hereInfo est alors connu car lancé avant et aussi lancer dans le meme step
    def mapper_load_names(self):
        self.heroInfo = {}
        with open("Marvel-names.txt") as f:
            for line in f:
                fields = unicode(line, errors='replace').split('"')
                heroID = int(fields[0])
                self.heroInfo[heroID] = fields[1]
                
    def mapper_sort_by_friends(self, heroID, numFriends):
        heroName = self.heroInfo[heroID]
        yield None, ("%04d"%int(numFriends), heroName)
    
    def reducer_most_friend(self, _, values):
        yield max(values)
    
    
    
if __name__ == "__main__":
    MRMostPopularSuperHero.run()
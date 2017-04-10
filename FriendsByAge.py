from mrjob.job import MRJob

class MRFriendsByAge(MRJob):
    
    def mapper(self, key, line):
        (id, name, age, friendNumber) = line.split(",")
        yield age, int(friendNumber)
        
    def reducer(self, age, friendNumberOccurences):
        numElement = 0
        totalFriend = 0
        for i in friendNumberOccurences:
            numElement += 1
            totalFriend += i
        
        yield age, totalFriend / numElement 
        
if __name__ == "__main__":
    MRFriendsByAge.run()
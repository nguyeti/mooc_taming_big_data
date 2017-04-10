from mrjob.job import MRJob
from mrjob.step import MRStep
# 44,8602,37.19

class MROrderAmountByCustomer(MRJob):
    
    def steps(self):
        return [ MRStep(mapper=self.mapper, reducer=self.reducer),
        MRStep(mapper=self.mapper2,reducer=self.reducer2)]
        
    
    def mapper(self, _, line):
        (user_id, order_id, amount) = line.split(",")
        yield user_id, float(amount)
        
    def reducer(self, user_id, amount):
        yield user_id, "%.2f"%sum(amount)
     
    def mapper2(self, user_id, amount):
        yield "%04.2f"%float(amount), user_id

    def reducer2(self,amount,user_ids):
        for user in user_ids:
            yield user, amount
              
if __name__ == "__main__":
    MROrderAmountByCustomer.run()
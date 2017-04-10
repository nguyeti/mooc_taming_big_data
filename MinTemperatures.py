from mrjob.job import MRJob

class MRMinTemperatures(MRJob):
    def toCelcius(self, temp):
        return float(temp)/10.0
        
    def mapper(self, key, line):
        (location, date, type, temp, a, b, c, d) = line.split(",")
        
        if type == "TMIN": 
            yield location, self.toCelcius(temp)
            
    def reducer(self, location, temps):
        yield location, min(temps)
        
if __name__ == "__main__":
    MRMinTemperatures.run()
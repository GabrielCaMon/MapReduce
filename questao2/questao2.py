from mrjob.job import MRJob
from mrjob.step import MRStep

class Heroes(MRJob):
	def mapper(self, _,line):
		lines = line.split(' ')
		heroes_id = lines[0]
		heroes_friends_count = len(lines) - 2
		yield heroes_id, heroes_friends_count
	def reducerSum (self,heroes_id,values):
		yield None,(sum(values), heroes_id)
	
	def reducerMax (self,value_none,values):
		sort = list(sorted(values))
		maximo = max(sort)
		yield maximo[1], maximo[0]
	def steps(self):
		return [
			MRStep(mapper=self.mapper,
					reducer=self.reducerSum),
			MRStep(reducer=self.reducerMax)
		]
 

if __name__ == '__main__':
    Heroes.run()
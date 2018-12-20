from mrjob.job import MRJob
from mrjob.step import MRStep

class CustomersAmmount(MRJob):
	def mapper(self, _, line):
		(user_id, movie_id, rating, timestamp) = line.split('\t')
		yield movie_id, 1
	def reducerSum(self, movie_id, rating_count):
		yield None, (sum(rating_count),movie_id)
	def reducerAsc(self, _, values):
		lista = list(values)
		sort = list(sorted(lista))
		maximo = max(sort) 
		yield maximo[0], maximo[1]

	def steps(self):
		return[
			MRStep(mapper=self.mapper,
					reducer=self.reducerSum),
			MRStep(reducer=self.reducerAsc)
		]
	
if __name__=='__main__':
	CustomersAmmount.run()

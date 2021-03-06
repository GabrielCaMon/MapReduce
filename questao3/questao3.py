from mrjob.job import MRJob
from mrjob.step import MRStep

class CustomersAmmount(MRJob):
	def mapper(self, _, line):
		(user_id, movie_id, rating, timestamp) = line.split('\t')
		yield movie_id, 1
	def reducerSum(self, movie_id, rating_count):
		yield None, (sum(rating_count),movie_id)
	def reducerAsc(self, value_none, values):
		lista = list(values)
		sort = list(reversed(sorted(lista)))
		result =list(sort[:10])
		for index in result:
			yield index[0], index[1]
	def steps(self):
		return[
			MRStep(mapper=self.mapper,
					reducer=self.reducerSum),
			MRStep(reducer=self.reducerAsc)
		]
	
if __name__=='__main__':
	CustomersAmmount.run()

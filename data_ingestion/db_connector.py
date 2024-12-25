import psycopg2
from psycopg2.extras import execute_values

class DBconnector:
	def __init__(self, db_name, user, password, host='localhost', port=5432):
		self.connection = psycopg2.connect(
			db_name=db_name, user=user, passwor=password, host=host, port=port
		)
		self.connection.autocommit = True
	
	def insert_tweets(self, tweets):
		query = """
		INSERT INTO tweets (tweet_id, created_at, author_id, text, like_count, reply_count, retweet_count, view_count)
		VALUES %s ON CONFLICT (tweet_id) DO NOTHING;
		"""
		values = [
			(
				tweets["id"],
				tweets["created_at"],
				tweets["author_id"],
				tweets["text"],
				tweets["like_count"],
				tweets["reply_count"],
				tweets["retweet_count"],
				tweets["view_count"]
			)
			for tweet in tweets
		]
		with self.connection.cursor() as cursor:
			execute_values(cursor, query, values)
	
	def close(self):
		self.connection.close()
		
import os
import json
from operator import add
from pyspark import SparkContext


def gen_rdd(tweets_file):
    return sc.textFile(tweets_file).map(process_tweet)

def process_tweet(raw_tweet):
    try:
        tweet = json.loads(raw_tweet)
        return (tweet['user']['id'], 1)
    except:
        return (0, 0)

if __name__ == "__main__":
  sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "UsersPostRank", pyFiles=['usersPostRank.py'])

  dir_path = 'twitter'
  all_tweets = gen_rdd(os.path.join(dir_path, 't01'))
  for fnum in range(2, 71):
      file_name = os.path.join(dir_path, 't' + '%02d' % fnum)
      all_tweets = all_tweets.union(gen_rdd(file_name))
  for fnum in range(1, 86):
      file_name = os.path.join(dir_path, 'u' + '%02d' % fnum)
      all_tweets = all_tweets.union(gen_rdd(file_name))
  all_tweets = all_tweets.reduceByKey(add) \
          .map(lambda (user, post): (post, 1)) \
          .reduceByKey(add) \
          .sortByKey(False)
  for (post, count) in all_tweets.collect():
      print "\t".join([str(post), str(count)])


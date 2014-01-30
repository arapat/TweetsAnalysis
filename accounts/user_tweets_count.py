# Get the number of tweets each user posted

import json
from operator import add
from pyspark import SparkContext


def gen_rdd(tweets_file):
    return sc.textFile(tweets_file).flatMap(process_tweet)


def process_tweet(raw_tweet):
    try:
        tweet = json.loads(raw_tweet)
        return (tweet["user"]["id"], 1)
    except:
        return []


def user_tweets_counts(files):
    if len(files) == 0:
        return 0

    all_users = None
    for file_name in files:
        users = gen_rdd(file_name)
        if all_users:
            all_users = all_users.union(users)
        else:
            all_users = users

    all_users = all_users.reduceByKey(add) \
            .map(lambda (a, b): (b, 1)) \
            .reduceByKey(add) \
            .cache()

    print "Top 1000 active users:"
    all_users.sortByKey(ascending=False).take(1000)
    print "Top 1000 silence users:"
    all_users.sortByKey(ascending=True).take(1000)

    return all_users.count()


if __name__ == '__main__':
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "UserTweetsCount", pyFiles=['user_tweets_count.py'])
    dir_path = '/user/arapat/twitter/'
    # files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]
    files = [dir_path + 't01', dir_path + 't02']

    print "Total users:", word_counts(files)


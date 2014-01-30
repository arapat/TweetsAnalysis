#!/usr/bin/python

# Get the word counts

import json
from operator import add
from pyspark import SparkContext


def gen_rdd(tweets_file):
    return sc.textFile(tweets_file).flatMap(process_tweet)

def process_tweet(raw_tweet):
    try:
        tweet = json.loads(raw_tweet)
        return [(word, 1) for word in tweet['text'].split()]
    except:
        return []

def word_counts(files):
    if len(files) == 0:
        return 0

    all_words = None
    for file_name in files:
        words = gen_rdd(file_name)
        if all_words:
            all_words = all_words.union(words)
        else:
            all_words = words

    all_words = all_words.reduceByKey(add) \
            .map(lambda (a, b): (b, 1)) \
            .reduceByKey(add) \
            .sortByKey(ascending=True)
#            .map(lambda (a, b): (b, [a]))
#            .reduceByKey(add)
#            .sortByKey(ascending=False) \
#            .cache()
    
    print "Top 1000 words:"
    print all_words.take(1000)

    return all_words.count()


if __name__ == '__main__':
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "CountWords", pyFiles=['count_words.py'])
    dir_path = '/user/arapat/twitter/'
    files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]
    # files = [dir_path + 't01', dir_path + 't02']

    print "Total words:", word_counts(files)


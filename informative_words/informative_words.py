# -*- coding: utf-8 -*-
# Compute and rank the "informativeness" (JS-divergence) of words

# import json
import sys 
from operator import add
from pyspark import SparkContext
from math import log

word_counts_sum = None

def gen_pairs_rdd(tweets_file):
    return sc.textFile(tweets_file).flatMap(generate_pairs)


def generate_pairs(raw_tweet):
    try:
        raw_tweet = raw_tweet.split('\t')
        tokens = raw_tweet[0].split()
        tags = raw_tweet[1].split()
        w1s = set([token.lower() + '\t' + tag for token, tag in zip(tokens, tags) if tag in ['N', 'S', '^', 'Z', 'L', 'M']])
        w2s = set([token.lower() + '\t' + tag for token, tag in zip(tokens, tags)])
        # tweet = json.loads(raw_tweet)
        # text = tweet["text"]
        # words = set([word.strip() for word in text.split()])
        pairs = []
        for w1 in w1s:
            for w2 in w2s:
                if w1 != w2:
                    pairs.append(((w1, w2), 1))
        return pairs
    except:
        return []


def gen_word_counts_rdd(tweets_file):
    return sc.textFile(tweets_file).flatMap(generate_word_counts)


def generate_word_counts(raw_tweet):
    try:
        raw_tweet = raw_tweet.split('\t')
        tokens = raw_tweet[0].split()
        tags = raw_tweet[1].split()
        words = set([token.lower() + '\t' + tag for token, tag in zip(tokens, tags)])
        # tweet = json.loads(raw_tweet)
        # text = tweet["text"]
        # words = set([word.strip() for word in text.split()])
        return [(word, 1) for word in words]
    except:
        return []


def compute_pairs_jsd(word_pair):
    global word_counts_sum

    # Extract information
    w1 = word_pair[0]
    w1_count = word_pair[1][0]
    w2_count = word_pair[1][1]
    pair_counts = word_pair[1][2]
    wp_sum = word_pair[1][3]

    wc_sum = word_counts_sum
    delta_cp = wc_sum - wp_sum

    kld_tmp1 = 2.0 * pair_counts * delta_cp
    kld_tmp2 = wc_sum * pair_counts + wp_sum * w2_count - 2 * wp_sum * pair_counts 
    kld_tmp3 = 2.0 * wp_sum * (w2_count - pair_counts)
    kld_tmp4 = kld_tmp2
    p1 = float(pair_counts) / wp_sum
    p2 = float(w2_count - pair_counts) / delta_cp

    # case 1: the probability of "t"'s occurrence when "word" occurs
    kld1 = log(kld_tmp1 / kld_tmp2, 2) * p1

    # case 2: the probability of "t"'s occurrence when "word" is absent
    if w2_count == pair_counts:
        kld2 = 0.0
    else:
        kld2 = log(kld_tmp3 / kld_tmp4, 2) * p2

    return (w1, (kld1 + kld2, w2_count, delta_cp, w1_count))

    # when the probability of "t"'s occurrence when "word" occurs is 0
    # jsd = (jsd + float(ir_sum) / delta_cp) / 2.0


def get_jsd(files):
    def process_stat1(wp):
        w1 = wp[0]
        w2 = wp[1][0][0]
        pair_counts = wp[1][0][1]
        wp_sum = wp[1][1][0]
        w1_count = wp[1][1][1]
        return (w2, (w1, w1_count, pair_counts, wp_sum))

    def process_stat2(wp):
        w1 = wp[1][0][0]
        w1_count = wp[1][0][1]
        pair_counts = wp[1][0][2]
        wp_sum = wp[1][0][3]
        w2_count = wp[1][1]
        return (w1, (w1_count, w2_count, pair_counts, wp_sum))

    def add_pairs(a, b):
        return (a[0] + b[0], a[1] + b[1], a[2], a[3])

    global word_counts_sum

    if len(files) == 0:
        return 0

    # Obtain (word, occurrence_count) pairs
    all_word_counts = None
    for file_name in files:
        word_counts = gen_word_counts_rdd(file_name)
        if all_word_counts:
            all_word_counts = all_word_counts.union(word_counts)
        else:
            all_word_counts = word_counts
    all_word_counts = all_word_counts.reduceByKey(add)

    # Obtain the sum of occurrence_count of all words
    word_counts_sum = all_word_counts.map(lambda (a, b): b).sum()

    # Obtain ((w1, w2), pair_counts) for subsequent computation
    all_word_pairs = None
    for file_name in files:
        word_pairs = gen_pairs_rdd(file_name)
        if all_word_pairs:
            all_word_pairs = all_word_pairs.union(word_pairs)
        else:
            all_word_pairs = word_pairs

    # The filter is to avoid the long tail, but need some theoretical support. 
    all_word_pairs = all_word_pairs.reduceByKey(add) #.filter(lambda ((w1, w2), count): count > 2)

    # Obtain the sum of word pairs
    sum_word_pairs = all_word_pairs.map( \
            lambda ((w1, w2), pair_counts): (w1, pair_counts)) \
            .reduceByKey(add)

    # (w1_count, wp_sum)
    word_stat = sum_word_pairs.leftOuterJoin(all_word_counts)

    # Obtain the occurrence count of the two words in word pairs
    all_word_pairs = all_word_pairs.map( \
            lambda ((w1, w2), pair_counts): (w1, (w2, pair_counts))) \
            .leftOuterJoin(word_stat) \
            .map(process_stat1) \
            .leftOuterJoin(all_word_counts) \
            .map(process_stat2)

    # Compute the JS-divergence of each word
    jsd = all_word_pairs.map(compute_pairs_jsd) \
            .reduceByKey(add_pairs) \
            .map(lambda (w, (tmp_jsd, count_ir, delta, w1_count)): \
            (((tmp_jsd + float(word_counts_sum - count_ir - w1_count) / delta) / 2.0), w)) \
            .sortByKey(ascending=True)

    print "Stop words:"
    # for (div, word) in jsd.take(1000000):
    for (div, word) in jsd.collect():
        word = word.split('\t')
        token = word[0]
        tag = word[1]
        try:
            print "\t".join([token, tag, str(div)])
        except:
            print "\t".join(["[Codec unknown]", tag, str(div)])

    return jsd.count()


if __name__ == '__main__':
    reload(sys) 
    sys.setdefaultencoding('utf8') 
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "InformativeWords", pyFiles=['informative_words.py'])
    dir_path = '/user/arapat/twitter-tag/'
    # files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]
    files = [dir_path + 't01', dir_path + 't02']
    # files = ['/user/arapat/twitter-sample/t01']
    # files = ['/user/arapat/twitter-sample/tag']

    print get_jsd(files)


# Compute and rank the "informativeness" of words

import json
from operator import add
from pyspark import SparkContext


def gen_pairs_rdd(tweets_file):
    return sc.textFile(tweets_file).flatMap(generate_pairs)


def generate_pairs(raw_tweet):
    try:
        tweet = json.loads(raw_tweet)
        text = tweet["text"]
        words = set([word.strip() for word in text.split()])
        pairs = []
        for w1 in words:
            for w2 in words:
                if w1 != w2:
                    pairs.append(((w1, w2), 1))
        return pairs
    except:
        return []


def gen_word_counts_rdd(tweets_file):
    return sc.textFile(tweets_file).flatMap(generate_word_counts)


def generate_word_counts(raw_tweet):
    try:
        tweet = json.loads(raw_tweet)
        text = tweet["text"]
        words = set([word.strip() for word in text.split()])
        return [(word, 1) for word in words]
    except:
        return []


def count_tweets(tweets_file):
    return sc.textFile(tweets_file).map(validate_tweet).sum()


def validate_tweet(raw_tweet):
    try:
        json.loads(raw_tweet)["text"]
        return 1
    except:
        return 0


def compute_jsd(word):
    # Obtain all pairs contains "word"
    wp = all_word_pairs.filter(lambda ((w1, w2), count): w1 == word).collect()
    wp = [(w2, count) for ((w1, w2), count) in wp]

    # Obtain word counts
    wp_words = [t for (t, count) in wp] + [word]
    wp_words_count = all_word_counts.filter(lambda (word, count): word in wp_words).collect()
    wp_words_count = {word: count for (word, count) in wp_words_count}
    count_w = wp_words_count[word]

    wc_sum = word_counts_sum
    wp_sum = sum([count for (w, count) in wp])
    delta_cp = wc_sum - wp_sum
    ir_sum = word_counts_sum

    jsd = 0.0
    for (t, count_pairs) in wp:
        count_t = wp_words_count[t]

        kld_tmp1 = 2.0 * count_pairs * delta_cp
        kld_tmp2 = wc_sum * count_pairs + wp_sum * count_t - 2 * wp_sum * count_pairs 
        kld_tmp3 = 2.0 * wp_sum * (count_t - count_pairs) * delta_cp
        kld_tmp4 = kld_tmp2 * delta_cp
        p1 = float(count_pairs) / wc_sum
        p2 = float(count_t - count_pairs) / delta_cp

        # case 1: the probability of "t"'s occurrence when "word" occurs
        kld1 = log(kld_tmp1 / kld_tmp2, 2) * p1

        # case 2: the probability of "t"'s occurence when "word" is absent
        kld2 = log(kld_tmp3 / kld_tmp4, 2) * p2

        ir_sum -= count_t
        jsd += kld1 + kld2

    # when the probability of "t"'s occurrence when "word" occurs is 0
    jsd = (jsd + float(ir_sum) / delta_cp) / 2.0

    return (jsd, word)


def get_jsd(files):
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
    all_word_counts.reduceByKey(add).cache()

    # Obtain all words
    all_words = all_word_counts.map(lambda (a, b): a)

    # Obtain the sum of occurrence_count of all words
    word_counts_sum = all_word_counts.map(lambda (a, b): b).sum()
    
    # Obtain total number of (valid) tweets
    tweets_count = 0
    for file_name in files:
        tweets_count += count_tweets(file_name)

    # Obtain ((w1, w2), pair_counts) for subsequent computation
    all_word_pairs = None
    for file_name in files:
        word_pairs = generate_pairs(file_name)
        if all_word_pairs:
            all_word_pairs = all_word_pairs.union(word_pairs)
        else:
            all_word_pairs = word_pairs
    all_word_pairs.reduceByKey(add).cache()

    all_words.map(compute_jsd).sortByKey()
    print "Stop words:"
    for (jsd, word) in all_words.take(1000):
        print word, "\t", jsd

    return all_words.count()
    

if __name__ == '__main__':
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "InformativeWords", pyFiles=['informative_words.py'])
    dir_path = '/user/arapat/twitter/'
    # files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]
    files = [dir_path + 't01', dir_path + 't02']

    print "Total words:", get_jsd(files)


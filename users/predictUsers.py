# Note that the add operation in K-means algorithm is defined WITHOUT factors

import json
import os
import sys
from operator import add
from pyspark import SparkContext

import numpy as np
from scipy.sparse import csc_matrix
# from scipy.sparse import dok_matrix

def gen_rdd(tweets_file):
    return sc.textFile(tweets_file).map(process_tweet)


def process_tweet(raw_data):
    try:
        raw_data = raw_data.split('\t')
        raw_json = json.loads(raw_data[3])
        uid = raw_json['user']['id']
        text = [w.strip() for w in raw_data[0].split()]
        return (uid, (1, text))
    except:
        return (0, (0, []))


def clustering_user(files):

    w_dict = None
    w_len = 0
    kPoints = None

    sqsum = lambda v: np.sum([d * d for d in v.data])

    def get_centers(K):
        file_path = "/oasis/projects/nsf/csd181/arapat/project/twitter/scripts/users/centers.txt"
        f = open(file_path)
        for i in range(6):
            f.readline()
        separate = f.readline().strip()
        centers = []
        for i in range(K):
            row = []
            data = []
            raw = f.readline()
            while raw.strip() != separate:
                raw = raw.split()
                row.append(int(raw[0]))
                data.append(float(raw[2]))
                raw = f.readline()
            centers.append(csc_matrix((data, (row, [0] * len(row))), shape=(w_len, 1)))
            f.readline()
        return centers

    def normalize(user):
        uid = user[0]
        counter = float(user[1][0])
        tokens = sorted(user[1][1])

        vec = []
        tp = (tokens[0], 1)
        for i in range(1,len(tokens)):
            if tp[0] == tokens[i]:
                tp = (tp[0], tp[1] + 1)
            else:
                vec.append(tp)
                tp = (tokens[i], 1)
        vec.append(tp)
        vec = map(lambda (token, weight): (token, weight / counter), vec)
        return (uid, vec)


    def to_sparse(user):
        uid = user[0]
        vec = user[1]
        # mat = dok_matrix((w_len, 1), dtype = np.float32)

        row = []
        data = []
        for i in range(len(vec)):
            idx = None
            try:
                idx = w_dict[vec[i][0]]
            except:
                # Do nothing: word appears only once
                pass
            if idx:
                # mat[idx, 0] = vec[i][1]
                row.append(idx)
                data.append(vec[i][1])

        return (uid, csc_matrix((data, (row, [0] * len(row))), shape=(w_len, 1)))
        # return (uid, mat)


    def closestPoint(user):
        vec = user[1]
        bestIndex = 0
        closest = float("+inf")
        for i in range(len(kPoints)):
            tempDist = sqsum(vec - kPoints[i])
            print "****** (", user[0], i, ")", tempDist
            if tempDist < closest:
                closest = tempDist
                bestIndex = i
        # return (bestIndex, (vec, 1))
        return (bestIndex, (vec, 1, user[0]))


    def closestDist(user):
        vec = user[1]
        bestIndex = 0
        closest = float("+inf")
        dist = []
        for i in range(len(kPoints)):
            tempDist = sqsum(vec - kPoints[i])
            dist.append((i, tempDist))
        return (user[0], dist)

    if len(files) == 0:
        return 0

    # Ignore users that have fewer than MIN_POST tweets
    # MIN_POST = 7
    # MIN_OCCURS = 50
    MIN_POST = 0
    MIN_OCCURS = 0

    # For each account, put all the tokens of its tweets into one vector
    all_users = None
    for file_name in files:
        users = gen_rdd(file_name)
        if all_users:
            all_users = all_users.union(users)
        else:
            all_users = users

    # Normalize the vector
    all_users = all_users.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]), numPartitions = 64) \
            .filter(lambda (uid, (counter, tokens)): counter >= MIN_POST)

    # 4606200 words
    all_words = all_users.flatMap(lambda (uid, (counter, tokens)): [(w, 1) for w in tokens]) \
            .reduceByKey(add, numPartitions = 64) \
            .filter(lambda (w, count): count >= MIN_OCCURS) \
            .map(lambda (w, count): w).collect()
    w_len = len(all_words)
    w_dict = {all_words[i]: i for i in range(len(all_words))}

    # Construct features
    all_users = all_users.map(normalize).map(to_sparse)

    # Clustering (k-means)
    K = 30
    CONVERGE = 0.1
    MAXITER = 10
    temp_dist = 1.0
    kPoints = get_centers(K)

    print "Final centers:"
    for point in kPoints:
        print "=========="
        row, col = point.nonzero()
        for i,j in zip(row, col):
            print i,j,point[i,j]
        print "=========="
    print "w_len =", w_len

    print "Users:"
    for user in all_users.collect():
        print "=========="
        print "id:", user[0]
        row, col = user[1].nonzero()
        for i, j in zip(row, col):
            print i,j,user[1][i,j]
        print "=========="

    print '\n\nSample data'
    dist = all_users.map(closestDist)
    print dist.take(10)
    # print '583408765', dist.filter(lambda (a, b): a == 583408765).collect()

    print '\n\nSize of each group:'
    closest = all_users.map(closestPoint).cache()
    for i in range(30):
        # print "Group %02d count =" % i, closest.filter(lambda (index, others): index == i).count()
        print "Group %02d" % i
        # print closest.filter(lambda (index, others): index == i).collect()
        print [t[1][2] for t in closest.filter(lambda (index, others): index == i).collect()]

    return all_users.count()
 

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf8')
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "ClusteringUsersPartitions", pyFiles=['clusteringUsersFixed.py'])

    dir_path = '/user/arapat/twitter-tag/'
    files = ['/user/arapat/twitter-sample/tag100']
    # files = [dir_path + 't01']
    # files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]
    # files = [dir_path + 't%02d' % k for k in range(1, 10)]
    print clustering_user(files)


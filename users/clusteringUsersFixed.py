# Note that the add operation in K-means algorithm is defined WITHOUT factors

import json
from operator import add
from pyspark import SparkContext
from scipy.sparse import csc_matrix
from scipy.sparse import dok_matrix

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

        ij = [[w_dict[vec[i][0]] for i in range(len(vec))], [0] * len(vec)]
        data = [vec[i][1] for i in range(len(vec))]

        return (uid, csc_matrix((data, ij), shape=(w_len, 1)))
 

    def closestPoint(p, centers):
        bestIndex = 0
        closest = float("+inf")
        for i in range(len(centers)):
            tempDist = diff(p, centers[i]) # np.sum((p - centers[i]) ** 2)
            if tempDist < closest:
                closest = tempDist
                bestIndex = i
        return bestIndex


    if len(files) == 0:
        return 0

    # Ignore users that have fewer than MIN_POST tweets
    MIN_POST = 7

    # For each account, put all the tokens of its tweets into one vector
    all_users = None
    for file_name in files:
        users = gen_rdd(file_name)
        if all_users:
            all_users = all_users.union(users)
        else:
            all_users = users

    # Normalize the vector
    all_users = all_users.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .filter(lambda (uid, (counter, tokens)): counter >= MIN_POST)

    # 4606200 words
    all_words = all_users.flatMap(lambda (uid, (counter, tokens)): [(w, 1) for w in tokens]) \
            .reduceByKey(add) \
            .filter(lambda (w, count): count > 1) \
            .map(lambda (w, count): w).collect()
    w_len = len(all_words)
    w_dict = {all_words[i]: i for i in range(len(all_words))}

    # Construct features
    all_users = all_users.map(normalize).map(to_sparse)

    # Clustering (k-means)
    K = 10 # 800
    CONVERGE = 0.3
    temp_dist = 1.0
    kPoints = all_users.takeSample(True, K, 1) \
            .map(lambda (uid, vec): dok_matrix(vec))

    while temp_dist > CONVERGE:
        closest = data.map(
            lambda p : (closestPoint(p[1], kPoints), (p[1], 1)))
        pointStats = closest.reduceByKey(
            lambda (vec1, counter1), (vec2, counter2): (vec1 + vec2, counter1 + counter2))
        newPoints = pointStats.map(
            lambda (cid, (vec, counter)): (cid, vec / counter)).collect()

        tempDist = sum(np.sum((kPoints[cid] - vec) ** 2) for (cid, vec) in newPoints)

        for (cid, vec) in newPoints:
            kPoints[cid] = vec

    print "Final centers:"
    for point in kPoints:
        print point
    return all_users.count()
 

if __name__ == "__main__":
    sc = SparkContext("spark://ion-21-14.sdsc.edu:7077", "ClusteringUsers", pyFiles=['clusteringUsers.py'])

    dir_path = '/user/arapat/twitter-tag/'
    files = [dir_path + 't01', dir_path + 't02']
    # files = [dir_path + 't%02d' % k for k in range(1, 71)] + [dir_path + 'u%02d' % k for k in range(1,86)]
    print clustering_user(files)


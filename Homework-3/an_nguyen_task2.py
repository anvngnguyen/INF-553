import sys
import time
from itertools import combinations
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS, Rating


def process_data(spark, train_file, test_file):
    # 1. Process training data
    train_text = spark.sparkContext.textFile(train_file)
    header = train_text.first()
    train_raw_data = train_text.filter(lambda row: row != header).map(lambda x: x.split(","))
    train_users = train_raw_data.map(lambda x: x[0]).distinct().collect()
    train_businesses = train_raw_data.map(lambda x: x[1]).distinct().collect()

    # 2. Process testing data
    test_text = spark.sparkContext.textFile(test_file)
    header = test_text.first()
    test_raw_data = test_text.filter(lambda row: row != header).map(lambda x: x.split(","))
    test_users = test_raw_data.map(lambda x: x[0]).distinct().collect()
    test_businesses = test_raw_data.map(lambda x: x[1]).distinct().collect()

    # 3. Create User/Business Dictionary and convert User/Business ID from String to Integer
    users = list(set(train_users) | set(test_users))
    users_dict = {j: i for i, j in enumerate(users)}
    businesses = list(set(train_businesses) | set(test_businesses))
    businesses_dict = {j: i for i, j in enumerate(businesses)}
    train_data = train_raw_data.map(lambda x: Rating(users_dict[x[0]], businesses_dict[x[1]], float(x[2])))
    test_data = test_raw_data.map(lambda x: Rating(users_dict[x[0]], businesses_dict[x[1]], float(x[2])))

    # 4. Create Test Keys for Predict Step
    test_data_keys = test_data.map(lambda x: (x[0], x[1]))

    return users, businesses, train_data, test_data, test_data_keys


def adjust_prediction(x):
    rating = 5 if x[2] > 5 else 1 if x[2] < 1 else x[2]
    return (x[0], x[1]), rating


def model_based_cf(train_data, test_data_keys, test_data):
    model = ALS.train(train_data, rank=2, iterations=5, lambda_=0.25, nonnegative=True)
    predictions = model.predictAll(test_data_keys).map(adjust_prediction)
    no_predictions = test_data \
        .map(lambda x: ((x[0], x[1]), None)) \
        .map(lambda x: (x[0], None)) \
        .subtractByKey(predictions) \
        .map(lambda x: (x[0], 3))
    predictions = predictions.union(no_predictions)

    return predictions


def user_rating_combination(x):
    for i in combinations(sorted(x[1]), 2):
        yield ((i[0][0], i[1][0]), (i[0][1], i[1][1]))


def pearson_correlation(pair_ratings):
    ratings_one = [r[0] for r in pair_ratings]
    ratings_two = [r[1] for r in pair_ratings]
    average_one = sum(ratings_one) / len(ratings_one) if len(ratings_one) > 0 else 0
    average_two = sum(ratings_two) / len(ratings_two) if len(ratings_two) > 0 else 0
    difference_one = [r - average_one for r in ratings_one]
    difference_two = [r - average_two for r in ratings_two]
    numerator = sum([i * j for i, j in zip(difference_one, difference_two)])
    denominator = (sum([d ** 2 for d in difference_one]) * sum([d ** 2 for d in difference_two])) ** 0.5
    return 0 if denominator == 0 else numerator / denominator


def user_based_predict(x, all_users_all_ratings_dict, all_users_avg_rating_dict, all_businesses_all_ratings_dict):
    user, business = x
    r_a = all_users_avg_rating_dict[user] if user in all_users_avg_rating_dict.keys() else 3
    user_all_rating = all_users_all_ratings_dict[user] if user in all_users_all_ratings_dict else dict()
    business_rating = all_businesses_all_ratings_dict[business] \
        if business in all_businesses_all_ratings_dict.keys() else dict()

    total = 0
    weights = []
    for neighbor, neighbor_rating in business_rating.items():
        neighbor_all_ratings = all_users_all_ratings_dict[neighbor] \
            if neighbor in all_users_all_ratings_dict else dict()
        pair_ratings = []
        for b in user_all_rating.keys():
            if b in neighbor_all_ratings.keys():
                pair_ratings.append((user_all_rating[b], neighbor_all_ratings[b]))
        correlation = pearson_correlation(pair_ratings)
        total += (neighbor_rating - all_users_avg_rating_dict[neighbor]) * correlation
        weights.append(abs(correlation))
    weight_sum = sum(weights)
    prediction = r_a + (total / weight_sum if weight_sum != 0 else 0)
    return user, business, prediction


def user_based_cf(train_data, test_data_keys):
    all_users_avg_rating = train_data \
        .map(lambda x: (x[0], x[2])) \
        .groupByKey() \
        .map(lambda x: (x[0], sum(x[1]) / len(x[1])))
    all_users_avg_rating_dict = dict(all_users_avg_rating.collect())
    all_businesses_all_ratings = train_data.map(lambda x: (x[1], [(x[0], x[2])])).reduceByKey(lambda x, y: x + y)
    all_businesses_all_ratings_dict = {i: dict(j) for i, j in all_businesses_all_ratings.collect()}
    all_user_all_ratings = train_data.map(lambda x: (x[0], [x[1], x[2]])).groupByKey()
    all_user_all_ratings_dict = {i: dict(j) for i, j in all_user_all_ratings.collect()}
    predictions = test_data_keys \
        .map(lambda x: user_based_predict(x, all_user_all_ratings_dict,
                                          all_users_avg_rating_dict, all_businesses_all_ratings_dict)) \
        .map(adjust_prediction)
    return predictions


HASH_VALUES = (
    (1, 16, 193), (11, 49, 1543), (19, 100, 12289), (37, 196, 196613),
    (3, 25, 389), (13, 64, 3079), (23, 121, 24593), (41, 225, 393241),
    (5, 36, 769), (17, 81, 6151), (31, 144, 98317), (43, 256, 786433),
    (1, 25, 769), (11, 64, 6151), (19, 121, 98317), (37, 225, 786433),
    (3, 36, 193), (13, 81, 1543), (23, 144, 12289), (41, 256, 196613),
    (5, 16, 389), (17, 49, 3079), (31, 100, 24593), (43, 196, 393241),
    (1, 36, 389), (11, 81, 3079), (19, 144, 24593), (37, 256, 393241),
    (3, 16, 769), (13, 49, 6151), (23, 100, 98317), (41, 196, 786433),
    (5, 25, 193), (17, 64, 1543), (31, 121, 12289), (43, 225, 196613),
)
N = len(HASH_VALUES)
BANDS = 18
ROWS = int(N / BANDS)


def min_hash(values, bin_count, hash_values):
    a, b, p = hash_values
    return min([((a * x + b) % p) % bin_count for x in values])


def create_candidate_group(signature):
    bins = []
    for i in range(BANDS):
        bins.append(((i, tuple(signature[1][i * ROWS:(i + 1) * ROWS])), signature[0]))
    return bins


def create_candidate_pairs(candidate_group):
    return combinations(candidate_group[1], 2)


def jaccard(pair, input_matrix):
    a = set(input_matrix[pair[0]])
    b = set(input_matrix[pair[1]])
    intersection = a & b
    union = a | b
    return pair[0], pair[1], len(intersection) / len(union)


def jaccard_lsh(data):
    users = data.map(lambda x: x[0]).distinct().collect()
    users_dict = {u: i for i, u in enumerate(users)}
    input_matrix = data.map(lambda x: (x[1], users_dict[x[0]])).groupByKey().sortBy(lambda x: x[0])
    input_matrix_dict = {i: j for i, j in input_matrix.collect()}

    bin_count = int(len(users) / N)
    signatures = input_matrix.map(lambda x: [x[0], [min_hash(x[1], bin_count, h) for h in HASH_VALUES]])

    groups = signatures.flatMap(create_candidate_group).groupByKey().filter(lambda x: len(x[1]) > 1)
    pairs = groups.flatMap(create_candidate_pairs).distinct()
    predictions = pairs.map(lambda x: jaccard((x[0], x[1]), input_matrix_dict)).filter(lambda x: x[2] >= 0.5)

    return predictions


def item_based_predict(x, all_users_all_ratings_dict, all_businesses_all_ratings_dict, similar_neighbors):
    user, business = x
    business_all_ratings = all_businesses_all_ratings_dict[business] \
        if business in all_businesses_all_ratings_dict else dict()
    user_all_ratings = all_users_all_ratings_dict[user] if user in all_users_all_ratings_dict else dict()
    if user in similar_neighbors:
        neighbors = set(user_all_ratings.keys()) & set(similar_neighbors[user])
    else:
        neighbors = set()

    total = 0
    weights = []
    correlation_list = []
    for n in neighbors:
        n_all_ratings = all_businesses_all_ratings_dict[n] if n in all_businesses_all_ratings_dict else {}
        n_rating = user_all_ratings[n]
        pair_ratings = []
        for b in n_all_ratings.keys():
            if b in business_all_ratings.keys():
                pair_ratings.append((n_all_ratings[b], business_all_ratings[b]))
        correlation = pearson_correlation(pair_ratings)
        correlation_list.append((correlation, n_rating))
    while len(correlation_list) > 0:
        temp = correlation_list[0]
        correlation_list.remove(temp)
        if temp[0] > 0:
            total += temp[0] * temp[1]
            weights.append(abs(temp[0]))
    weight_sum = sum(weights)
    rating = total / weight_sum if weight_sum != 0 else 3
    return user, business, rating


def item_based_cf(train_data, test_data_keys):
    all_businesses_all_ratings = train_data.map(lambda x: (x[1], [(x[0], x[2])])).reduceByKey(lambda x, y: x + y)
    all_businesses_all_ratings_dict = {i: dict(j) for i, j in all_businesses_all_ratings.collect()}
    all_users_all_ratings = train_data.map(lambda x: (x[0], [x[1], x[2]])).groupByKey()
    all_users_all_ratings_dict = {i: dict(j) for i, j in all_users_all_ratings.collect()}

    lsh_result = jaccard_lsh(train_data).collect()
    similar_neighbors = {}
    for i, j, _ in lsh_result:
        if i not in similar_neighbors.keys():
            similar_neighbors[i] = []
        similar_neighbors[i].append(j)
        if j not in similar_neighbors.keys():
            similar_neighbors[j] = []
        similar_neighbors[j].append(i)
    predictions = test_data_keys \
        .map(lambda x: item_based_predict(x, all_users_all_ratings_dict,
                                          all_businesses_all_ratings_dict, similar_neighbors)) \
        .map(adjust_prediction)

    return predictions


def output_to_file(output_path, users, businesses, predictions):
    final_result = predictions.map(lambda x: (users[x[0][0]], businesses[x[0][1]], str(x[1]))).collect()
    with open(output_path, "w") as f:
        f.write("user_id, business_id, prediction\n")
        for line in final_result:
            f.write(",".join(line) + "\n")


def main():
    start = time.time()
    train_file, test_file, case_id, output_path = sys.argv[1:]
    if case_id != "1" and case_id != "2" and case_id != "3":
        print("Invalid Case Id!")
        return

    spark = SparkSession.builder.master("local[*]").appName("HW3-Task2").getOrCreate()
    spark.conf.set("spark.driver.memory", "32g")
    spark.sparkContext.setLogLevel("WARN")

    users, businesses, train_data, test_data, test_data_keys = process_data(spark, train_file, test_file)
    if case_id == "1":
        predictions = model_based_cf(train_data, test_data_keys, test_data)
    elif case_id == "2":
        predictions = user_based_cf(train_data, test_data_keys)
    else:
        predictions = item_based_cf(train_data, test_data_keys)
    output_to_file(output_path, users, businesses, predictions)

    spark.stop()
    print(f"Duration: {time.time() - start}")


main()

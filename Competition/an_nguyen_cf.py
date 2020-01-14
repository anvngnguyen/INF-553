import sys
import time
from pyspark.sql import SparkSession


def extract_data_from_file(sc, file):
    text = sc.textFile(file)
    header = text.first()
    data = text.filter(lambda row: row != header).map(lambda s: s.split(",")).map(lambda x: (x[0], x[1], float(x[2])))
    return data


def process_data(sc, train_file, test_file):
    train_data = extract_data_from_file(sc, train_file)
    test_data = extract_data_from_file(sc, test_file)
    return train_data, test_data


def avg_rating(ratings):
    return sum(ratings) / len(ratings) if len(ratings) > 0 else 0


def get_user_avg_rating_dict(train_data):
    return train_data.map(lambda x: (x[0], x[2])).groupByKey().map(lambda x: (x[0], avg_rating(x[1]))).collectAsMap()


def get_business_avg_rating(train_data):
    return train_data.map(lambda x: (x[1], x[2])).groupByKey().map(lambda x: (x[0], avg_rating(x[1]))).collectAsMap()


def get_user_rating_on_business_dict(train_data):
    user_rating_on_business_dict = train_data.map(lambda x: (x[1], (x[0], x[2]))).groupByKey().collectAsMap()
    for business, user_rating in user_rating_on_business_dict.items():
        temp = {}
        for user, rating in user_rating:
            temp[user] = rating
        user_rating_on_business_dict[business] = temp
    return user_rating_on_business_dict


def pearson_correlation(business, user_rating_on_business_dict):
    weights = []
    for target in user_rating_on_business_dict.keys():
        if target == business:
            continue
        user_of_business_one = set(user_rating_on_business_dict[business].keys())
        user_of_business_two = set(user_rating_on_business_dict[target].keys())
        joined_user = user_of_business_one & user_of_business_two
        if len(joined_user) > 0:
            rating_one = [user_rating_on_business_dict[business][user] for user in joined_user]
            avg_one = sum(rating_one) / len(rating_one)
            rating_two = [user_rating_on_business_dict[target][user] for user in joined_user]
            avg_two = sum(rating_two) / len(rating_two)
            numerator = sum([(i - avg_one) * (j - avg_two) for i, j in zip(rating_one, rating_two)])
            denominator = (sum([(i - avg_one) ** 2 for i in rating_one])) ** 0.5 * (
                sum([(i - avg_two) ** 2 for i in rating_two])) ** 0.5
            weight = numerator / denominator if denominator > 0 else 0
        else:
            weight = 0
        weights.append((target, weight))
    return weights


def predict_rating(business, users, user_rating_on_business_dict, user_avg_rating_dict, business_avg_rating_dict):
    prediction = []
    weights = []
    if business in business_avg_rating_dict:
        weights = sorted(pearson_correlation(business, user_rating_on_business_dict), key=lambda tup: -tup[1])[:5]
    for user in users:
        rating = user_avg_rating_dict[user] if user in user_avg_rating_dict else 3.0
        if user in user_avg_rating_dict and business in business_avg_rating_dict:
            total_rating = 0.0
            for neighbor, w in weights:
                condition = user in user_rating_on_business_dict[neighbor].keys()
                user_rating = user_rating_on_business_dict[neighbor][user] if condition else user_avg_rating_dict[user]
                total_rating += user_rating * w
            sum_weights = sum([abs(w) for _, w in weights])
            rating = total_rating / sum_weights if sum_weights > 0 else 3.0
        prediction.append([user, business, rating])
    return prediction


def main():
    start = time.time()
    input_path, test_file, result_file = sys.argv[1:]
    train_file = input_path + "/yelp_train.csv"

    spark = SparkSession.builder.master("local[*]").appName("Competition").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    train_data, test_data = process_data(sc, train_file, test_file)
    test_keys = test_data.map(lambda x: (x[1], x[0])).groupByKey()

    user_avg_rating_dict = get_user_avg_rating_dict(train_data)
    business_avg_rating_dict = get_business_avg_rating(train_data)
    user_rating_on_business_dict = get_user_rating_on_business_dict(train_data)
    prediction = test_keys.map(
        lambda x: predict_rating(x[0], x[1], user_rating_on_business_dict, user_avg_rating_dict, business_avg_rating_dict))
    with open(result_file, "w") as f:
        f.write("user_id, business_id, prediction")
        for p in prediction.collect():
            for user, business, rating in p:
                f.write(f"\n{user},{business},{rating}")

    spark.stop()
    print(f"Duration: {time.time() - start}")


main()

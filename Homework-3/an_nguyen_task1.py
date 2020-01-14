import sys
import time
from itertools import combinations
from pyspark.sql import SparkSession

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
    # (7, 16, 193), (23, 49, 1543), (37, 100, 12289), (19, 196, 196613),
    # (7, 25, 389), (23, 64, 3079), (37, 121, 24593), (19, 225, 393241),
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


def cosine(pair, input_matrix):
    a = set(input_matrix[pair[0]])
    b = set(input_matrix[pair[1]])
    union = a | b
    vector_a = [1 if i in a else 0 for i in union]
    vector_b = [1 if i in b else 0 for i in union]
    numerator = sum([i * j for i, j in zip(vector_a, vector_b)])
    denominator = (sum([i ** 2 for i in a]) ** 0.5) * (sum([i ** 2 for i in b]) ** 0.5)
    return numerator / denominator if denominator > 0 else 0


def cosine_lsh(data):
    users = data.map(lambda x: x[0]).distinct().collect()
    users_dict = {u: i for i, u in enumerate(users)}
    input_matrix = data.map(lambda x: (x[1], users_dict[x[0]])).groupByKey().sortBy(lambda x: x[0])
    input_matrix_dict = {i: j for i, j in input_matrix.collect()}

    bin_count = int(len(users) / N)
    signatures = input_matrix.map(lambda x: [x[0], [min_hash(x[1], bin_count, h) for h in HASH_VALUES]])

    groups = signatures.flatMap(create_candidate_group).groupByKey().filter(lambda x: len(x[1]) > 1)
    pairs = groups.flatMap(create_candidate_pairs).distinct()
    predictions = pairs.map(lambda x: cosine((x[0], x[1]), input_matrix_dict)).filter(lambda x: x[2] >= 0.5)

    return predictions


def main():
    start = time.time()
    spark = SparkSession.builder.master("local[*]").appName("HW3-Task1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    input_file, similarity_method, output_path = sys.argv[1:]
    text = spark.sparkContext.textFile(input_file)
    header = text.first()
    data = text.filter(lambda row: row != header).map(lambda x: x.split(","))

    if similarity_method == "jaccard":
        result = jaccard_lsh(data)
    elif similarity_method == "cosine":
        result = cosine_lsh(data)
    else:
        print("Invalid Similarity Method!")
        return

    # Write result to file
    final_result = result.sortBy(lambda x: (x[0], x[1])).collect()
    with open(output_path, "w+") as f:
        f.write("business_id_1, business_id_2, similarity" + "\n")
        for r in final_result:
            f.write(r[0] + "," + r[1] + "," + str(r[2]) + "\n")

    spark.stop()
    print(f"Duration: {time.time() - start}")


main()

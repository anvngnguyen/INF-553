import sys
import json
import binascii
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

HASH_FUNCTIONS = (
    (1, 16, 193), (11, 49, 1543), (19, 100, 12289), (37, 196, 196613),
    (3, 25, 389), (13, 64, 3079), (23, 121, 24593), (41, 225, 393241),
    (5, 36, 769), (17, 81, 6151), (31, 144, 98317), (43, 256, 786433),
    (1, 25, 769), (11, 64, 6151), (19, 121, 98317), (37, 225, 786433),
    (3, 36, 193), (13, 81, 1543), (23, 144, 12289), (41, 256, 196613),
    (5, 16, 389), (17, 49, 3079), (31, 100, 24593), (43, 196, 393241),
    (1, 36, 389), (11, 81, 3079), (19, 144, 24593), (37, 256, 393241),
    (3, 16, 769), (13, 49, 6151), (23, 100, 98317), (41, 196, 786433),
    (5, 25, 193), (17, 64, 1543), (31, 121, 12289), (43, 225, 196613)
)
NUM_GROUPS = 4
ENTRIES_PER_GROUP = int(len(HASH_FUNCTIONS) / NUM_GROUPS)
M = 200


def count_trailing_zeroes(binary_string: str):
    return len(binary_string) - len(binary_string.rstrip("0"))


def flajolet_martin(time, rdd):
    global f
    global output_file

    actual_distinct_cities = set()
    hash_binary_bins = []

    data = rdd.collect()
    for i in data:
        i = i.strip()
        json_i = json.loads(i)
        city = json_i["city"]
        city_code = int(binascii.hexlify(city.encode('utf8')), 16)
        actual_distinct_cities.add(city)

        hash_binary_values = []
        for a, b, p in HASH_FUNCTIONS:
            value = ((a * city_code + b) % p) % M
            binary_value = bin(value)[2:]  # Strip "0b"
            hash_binary_values.append(binary_value)
        hash_binary_bins.append(hash_binary_values)

    estimates = []
    for i in range(len(HASH_FUNCTIONS)):
        r = max([count_trailing_zeroes(binary_values[i]) for binary_values in hash_binary_bins])
        estimates.append(2 ** r)

    average_estimates = []
    for i in range(NUM_GROUPS):
        total = 0
        for j in range(ENTRIES_PER_GROUP):
            total += estimates[i * ENTRIES_PER_GROUP + j]
        average_estimates.append(total / ENTRIES_PER_GROUP)
    average_estimates.sort()
    median = average_estimates[int(NUM_GROUPS / 2)]

    f = open(output_file, "a")
    f.write(f"\n{time},{len(actual_distinct_cities)},{median}")
    f.close()


port, output_file = sys.argv[1:]
f = open(output_file, "w")
f.write("Time,Ground Truth,Estimation")
f.close()

spark = SparkSession.builder.master("local[*]").appName("HW5").getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream("localhost", int(port)).window(30, 10)
lines.foreachRDD(flajolet_martin)
ssc.start()
ssc.awaitTermination()

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
M = 200


def bloom_filtering(time, rdd):
    global false_positive
    global true_negative
    global filter_bit_array
    global f
    global output_file

    data = rdd.collect()
    for i in data:
        i = i.strip()
        json_i = json.loads(i)
        state = json_i["state"]
        state_code = int(binascii.hexlify(state.encode('utf8')), 16)
        hash_values = [((a * state_code + b) % p) % M for a, b, p, in HASH_FUNCTIONS]

        counter = sum([1 for value in hash_values if filter_bit_array[value] == 1])
        if state not in seen_states:
            if counter == len(hash_values):
                false_positive += 1
            else:
                true_negative += 1

        for value in hash_values:
            filter_bit_array[value] = 1
        seen_states.add(state)

    f = open(output_file, "a")
    fpr = float(false_positive / (false_positive + true_negative))
    f.write(f"\n{time},{fpr}")
    f.close()


seen_states = set()
false_positive = 0
true_negative = 0
filter_bit_array = [0 for _ in range(M)]
port, output_file = sys.argv[1:]
f = open(output_file, "w")
f.write("Time,FPR")
f.close()

spark = SparkSession.builder.master("local[*]").appName("HW5").getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("localhost", int(port))
lines.foreachRDD(bloom_filtering)
ssc.start()
ssc.awaitTermination()

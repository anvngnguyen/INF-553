import sys
import json
import time
from typing import List
from pyspark.sql import SparkSession


def average(a_list: List[int]) -> float:
    return sum(a_list) / len(a_list)


def main(review_input, business_input, output_one, output_two):
    spark = SparkSession.builder.master("local[*]").appName("HW1").getOrCreate()
    reviews = spark             \
        .sparkContext           \
        .textFile(review_input) \
        .coalesce(8)            \
        .map(json.loads)        \
        .map(lambda x: (x["business_id"], x["stars"]))
    businesses = spark              \
        .sparkContext               \
        .textFile(business_input)   \
        .coalesce(8)                \
        .map(json.loads)            \
        .map(lambda x: (x["business_id"], x["state"]))
    joined_data = reviews                               \
        .join(businesses)                               \
        .coalesce(2)                                    \
        .map(lambda x: [x[1][1], x[1][0]])              \
        .groupByKey()                                   \
        .map(lambda x: [x[0], average(x[1])])           \
        .sortBy(lambda x: (-x[1], x[0]))

    """ Task B """
    result = {}
    #   Method 1: Collect all the data, and then print the first 5 states
    start = time.time()
    method_one_data = joined_data.collect()
    for d in method_one_data[:5]:
        print(d)
    process_time = time.time() - start
    result["m1"] = process_time
    #   Method 2: Take the first 5 states, and then print them
    start = time.time()
    method_two_data = joined_data.take(5)
    for d in method_two_data:
        print(d)
    process_time = time.time() - start
    result["m2"] = process_time
    #   Explanation:
    result["explanation"] = "The first() method will be faster since it only return a portion of the RDD (in this" + \
                            " case is 5) comparing to the collect() which will return everything. Hence, Method B" + \
                            " will take a lot less time than Method A."
    with open(output_two, "w", encoding="utf-8") as f:
        json.dump(result, f)

    """ Task A """
    with open(output_one, "w", encoding="utf-8") as f:
        f.write("state,stars\n")
        for item in method_one_data:
            f.write(f"{item[0]},{item[1]}\n")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

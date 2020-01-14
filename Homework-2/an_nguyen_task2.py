import sys
import time
from typing import List, Iterator
from pyspark.sql import SparkSession


def process_data(spark, input_file):
    raw_text = spark.sparkContext.textFile(input_file)
    text = raw_text.map(lambda x: x.split(","))
    head = text.first()
    raw_data = text.filter(lambda row: row != head)
    data = raw_data.map(lambda x: (x[0] + "-" + x[1]).replace("\"", "") + "," + x[5].replace("\"", ""))
    return data


def get_user_input():
    filter_threshold, min_count, input_file, output_file = sys.argv[1:]
    filter_threshold = int(filter_threshold)
    min_count = int(min_count)
    return filter_threshold, min_count, input_file, output_file


def a_priori(chunk: Iterator[List[str]], min_count: int, number_of_partition: int) -> tuple:
    partition_min_count = min_count / number_of_partition

    occurrences = {}
    baskets = []
    for c in chunk:
        baskets.append(c)
        for item in c:
            if item not in occurrences:
                occurrences[item] = 0
            occurrences[item] += 1

    frequent_singletons = sorted([k for k, v in occurrences.items() if v >= partition_min_count])
    yield 1, frequent_singletons

    k = 2
    frequent_items = [([i]) for i in frequent_singletons]
    while True:
        new_candidates = {tuple(sorted(set(i) | set(j))): 0
                          for i in frequent_items for j in frequent_items if len(set(i) | set(j)) == k}
        if len(new_candidates) > 0:
            for candidate in new_candidates.keys():
                for basket in baskets:
                    if all(1 if i in basket else 0 for i in candidate):
                        new_candidates[candidate] += 1
            frequent_items = [k for k, v in new_candidates.items() if v >= partition_min_count]
            if len(frequent_items) > 0:
                yield k, sorted(frequent_items)
                k += 1
            else:
                break
        else:
            break


def count_occurrences(chunk: List[str], candidate: List) -> tuple:
    occurrences = {}
    for basket in chunk:
        for c in candidate:
            for item_set in c[1]:
                if item_set in basket or (type(item_set) != int and all([i in basket for i in item_set])):
                    if item_set not in occurrences.keys():
                        occurrences[item_set] = 0
                    occurrences[item_set] += 1

    for k in occurrences.keys():
        item_set = k if type(k) == tuple else tuple([k])
        yield item_set, occurrences[k]


def main():
    start = time.time()

    spark = SparkSession.builder.master("local[*]").appName("HW2").getOrCreate()
    filter_threshold, min_count, input_file, output_file = get_user_input()
    data = process_data(spark, input_file)
    baskets = data \
        .map(lambda line: (line.split(',')[0], int(line.split(',')[1]))) \
        .groupByKey() \
        .map(lambda x: list(x[1])) \
        .filter(lambda x: len(x) > filter_threshold)

    number_of_partition = baskets.getNumPartitions()
    phase1 = baskets \
        .mapPartitions(lambda chunk: a_priori(chunk, min_count, number_of_partition)) \
        .reduceByKey(lambda a, b: sorted(set(a + b)))
    candidate = phase1.collect()
    with open(output_file, "w") as f:
        f.write("Candidates:\n")
        for c in sorted(candidate):
            if c[0] == 1:
                f.write(",".join([f"('{item}')" for item in c[1]]) + "\n")
            else:
                f.write(",".join([f"{item}" for item in c[1]]) + "\n")
            f.write("\n")

    phase2 = baskets \
        .mapPartitions(lambda chunk: count_occurrences(chunk, candidate)) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] >= min_count)
    result = phase2.collect()
    result = sorted(result, key=(lambda x: (len(x[0]), x[0])))
    temp = {}
    for r in result:
        if len(r[0]) not in temp.keys():
            temp[len(r[0])] = []
        temp[len(r[0])].append(r[0])
    with open(output_file, "a") as f:
        k = 0
        f.write("Frequent Itemsets:\n")
        for key in temp.keys():
            if key == 1:
                f.write(",".join([f"('{item[0]}')" for item in temp[key]]) + "\n")
            else:
                f.write(",".join([f"{item}" for item in temp[key]]) + "\n")
            k += 1
            if k < len(temp):
                f.write("\n")

    spark.stop()
    duration = time.time() - start
    print(f"Duration: {duration}")


main()

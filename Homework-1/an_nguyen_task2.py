import sys
import json
import time
from pyspark.sql import SparkSession


def main(input_file, output_file, num_of_partitions):
    result = {}
    spark = SparkSession.builder.master("local[*]").appName("HW1").getOrCreate()
    text = spark.sparkContext.textFile(input_file).cache()

    """ Default partition """
    start = time.time()
    default = text.coalesce(8)
    temp = default                                          \
        .map(json.loads)                                    \
        .map(lambda x: [x["user_id"], x["review_count"]])   \
        .map(lambda x: [x[0], x[1]])                        \
        .sortBy(lambda x: (-x[1], x[0]))                    \
        .take(10)
    size = default.glom().map(len).collect()
    process_time = time.time() - start
    result["default"] = {"n_partition": default.getNumPartitions(), "n_items": size, "exe_time": process_time}

    """ Customized partition """
    start = time.time()
    num_of_partitions = int(num_of_partitions)
    customized = text.repartition(num_of_partitions)
    temp = customized                                       \
        .map(json.loads)                                    \
        .map(lambda x: [x["user_id"], x["review_count"]])   \
        .map(lambda x: [x[0], x[1]])                        \
        .sortBy(lambda x: (-x[1], x[0]))                    \
        .take(10)
    size = customized.glom().map(len).collect()
    process_time = time.time() - start
    result["customized"] = {"n_partition": num_of_partitions, "n_items": size, "exe_time": process_time}

    """ Explanation """
    result["explanation"] = "In Default paritioning (including using coalesce), the size of each chunks is vastly" + \
                            " different while they are more uniformed when created by Customize partitioning" + \
                            " (using repartition). This is because rapartition will reshuffle and divide the data" + \
                            " equally while Default partitioning will just assign chunks of data based on their" + \
                            " location."

    with open(output_file, "w") as f:
        json.dump(result, f)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])

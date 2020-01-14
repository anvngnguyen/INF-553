import sys
import time
from graphframes import *
from pyspark.sql import SparkSession, Row


def main():
    start = time.time()

    input_filepath, output_filepath = sys.argv[1:]
    spark = SparkSession.builder.master("local[*]").appName("HW3-Task1").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    text_rdd = sc.textFile(input_filepath).map(lambda x: x.split(" "))
    text_rdd = text_rdd.flatMap(lambda x: ((int(x[0]), int(x[1])), (int(x[1]), int(x[0]))))
    edges_df = text_rdd.toDF(["src", "dst"])
    vertices_df = text_rdd.map(lambda x: x[0]).distinct().map(lambda x: Row(id=x)).toDF()

    g = GraphFrame(vertices_df, edges_df)
    result = g.labelPropagation(maxIter=5).collect()
    result_dict = {}
    for i in result:
        label = i["label"]
        if label not in result_dict.keys():
            result_dict[label] = []
        result_dict[label].append(str(i["id"]))

    ordered_keys = sorted(result_dict, key=lambda k: (len(result_dict[k]), min(result_dict[k])))
    with open(output_filepath, "w") as f:
        for key in ordered_keys:
            temp = sorted(result_dict[key])
            f.write("'" + "','".join(temp) + "'\n")

    spark.stop()
    print(f"Duration: {time.time() - start}")


main()

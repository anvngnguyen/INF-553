from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("HW3-Task2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

gt_text = spark.sparkContext.textFile("data/pure_jaccard_similarity.csv")
h1 = gt_text.first()
gt = gt_text.filter(lambda row: row != h1).map(lambda x: x.split(",")).map(lambda x: (x[0], x[1])).collect()
pr_text = spark.sparkContext.textFile("data/An_Nguyen_SimilarProducts_Jaccard.txt")
h2 = pr_text.first()
pr = pr_text.filter(lambda row: row != h2).map(lambda x: x.split(",")).map(lambda x: (x[0], x[1])).collect()

true_positive = set(pr) & set(gt)
print(f"Precision: {len(true_positive) / len(pr)}")
print(f"Recall: {len(true_positive) / len(gt)}")

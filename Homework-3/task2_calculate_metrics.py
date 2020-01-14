import sys
from pyspark.sql import SparkSession


ground_truth_file = "data/yelp_val.csv"
cf_type = sys.argv[1]
if cf_type == "1":
    check_file = "data/an_nguyen_ModelBasedCF.txt"
elif cf_type == "2":
    check_file = "data/an_nguyen_UserBasedCF.txt"
else:
    check_file = "data/an_nguyen_ItemBasedCF.txt"

spark = SparkSession.builder.master("local[*]").appName("HW3-Task2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

prediction_text = spark.sparkContext.textFile(check_file)
header = prediction_text.first()
prediction_raw_data = prediction_text.filter(lambda row: row != header).map(lambda x: x.split(","))
predictions = prediction_raw_data.map(lambda x: ((x[0], x[1]), float(x[2])))

ground_truth_text = spark.sparkContext.textFile(ground_truth_file)
h = ground_truth_text.first()
ground_truth_raw_data = ground_truth_text.filter(lambda row: row != h).map(lambda x: x.split(","))
ground_truth = ground_truth_raw_data.map(lambda x: ((x[0], x[1]), float(x[2])))

result = ground_truth.join(predictions)
diff = result.map(lambda r: abs(r[1][0] - r[1][1]))
diff01 = diff.filter(lambda x: 0 <= x < 1)
diff12 = diff.filter(lambda x: 1 <= x < 2)
diff23 = diff.filter(lambda x: 2 <= x < 3)
diff34 = diff.filter(lambda x: 3 <= x < 4)
diff4 = diff.filter(lambda x: 4 <= x)
r_mse = (diff.map(lambda x: x ** 2).mean()) ** 0.5
print(">=0 and <1: " + str(diff01.count()))
print(">=1 and <2: " + str(diff12.count()))
print(">=2 and <3: " + str(diff23.count()))
print(">=3 and <4: " + str(diff34.count()))
print(">=4: " + str(diff4.count()))
print("Root Mean Squared Error: " + str(r_mse))

spark.stop()

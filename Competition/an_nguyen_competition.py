import sys
import time
import pandas
from pyspark.sql import SparkSession
from sklearn import linear_model
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def sentiment_score(sentence):
    global analyzer
    if not isinstance(sentence, str):
        return 0
    sentiment_dict = analyzer.polarity_scores(sentence)
    return sentiment_dict["compound"]


start = time.time()
input_path, test_file, result_file = sys.argv[1:]
spark = SparkSession.builder.master("local[*]").appName("Competition").getOrCreate()

"""
Step 1: Reading and extracting data from training file (yelp_train.csv) and testing file (yelp_val.csv in this case)
Note:
- Remove the whitespace in the header that preceded business_id and stars
"""
column_names = {"user_id": "user_id", " business_id": "business_id", " stars": "stars"}
train_file = input_path + "/yelp_train.csv"
train_data = spark.read.csv(train_file, header=True)
test_data = spark.read.csv(test_file, header=True)
for old_name, new_name in column_names.items():
    train_data = train_data.withColumnRenamed(old_name, new_name)
    test_data = test_data.withColumnRenamed(old_name, new_name)

"""
Step 2: Reading and extracting data from user.json
"""
user_column_names = {"user_id": "user_id", "average_stars": "user_stars", "review_count": "user_review_count",
                     "useful": "user_useful", "funny": "user_funny", "cool": "user_cool", "fans": "user_fans"}
user_file = input_path + "/user.json"
user_data = spark.read.json(user_file).select(list(user_column_names.keys()))
for old_name, new_name in user_column_names.items():
    user_data = user_data.withColumnRenamed(old_name, new_name)

"""
Step 3: Reading and extracting data from business.json
"""
business_column_name = {"business_id": "business_id",
                        "stars": "business_stars", "review_count": "business_review_count",
                        "latitude": "business_latitude", "longitude": "business_longitude"}
business_file = input_path + "/business.json"
business_data = spark.read.json(business_file).select(list(business_column_name.keys()))
for old_name, new_name in business_column_name.items():
    business_data = business_data.withColumnRenamed(old_name, new_name)

"""
Step 4: Reading and extracting data from tip.json
"""
tip_column_name = {"user_id": "user_id", "business_id": "business_id", "text": "tip"}
tip_file = input_path + "/tip.json"
tip_data = spark.read.json(tip_file).select(list(tip_column_name.keys())).toPandas()

"""
Step 5: Combining data from business.json, user.json, and tip.json with yelp data. Also analyzing the text from the tip
to determine whether the user has positive/negative sentiment towards the business
"""
keys = ["user_id", "business_id"]
train_set = train_data.join(user_data, keys[0]).join(business_data, keys[1]).toPandas()
train_set = pandas.merge(train_set, tip_data, on=keys, how="left").drop_duplicates(subset=keys)
test_set = test_data.join(user_data, keys[0]).join(business_data, keys[1]).toPandas()
test_set = pandas.merge(test_set, tip_data, on=keys, how="left").drop_duplicates(subset=keys)
analyzer = SentimentIntensityAnalyzer()
train_set["sentiment"] = train_set.apply(lambda x: sentiment_score(x["text"]), axis=1)
test_set["sentiment"] = test_set.apply(lambda x: sentiment_score(x["text"]), axis=1)

"""
Step 7: Preparing training/testing features/labels
"""
dropped_columns = ["user_id", "business_id", "stars", "text"]
train_X, train_y = train_set.drop(columns=dropped_columns), train_set["stars"]
test_X = test_set.drop(columns=dropped_columns)
scaler_X = StandardScaler().fit(train_X)
train_X = scaler_X.transform(train_X)
test_X = scaler_X.transform(test_X)

"""
Step 8: Fitting and Predicting
Note:
- Using PolynomialFeatures of higher degree to transform the data to non-linear even though we are still using Linear
Regression
- If the predicted rating is out of range (1-5), change it back to within the accepted range
"""
polynomial_features = PolynomialFeatures(degree=3)
train_X = polynomial_features.fit_transform(train_X)
test_X = polynomial_features.fit_transform(test_X)
model = linear_model.LinearRegression()
model.fit(train_X, train_y)
test_set["prediction"] = model.predict(test_X)
for index, row in test_set.iterrows():
    if float(row.prediction) < 1.0:
        test_set["prediction"][index] = 1.0
    elif float(row.prediction) > 5.0:
        test_set["prediction"][index] = 5.0

"""
Step 9: Printing to file
"""
with open(result_file, "w") as f:
    f.write("user_id, business_id, prediction")
    for _, row in test_set.iterrows():
        f.write(f"\n{row.user_id},{row.business_id},{row.prediction}")

spark.stop()
print(f"Duration: {time.time() - start}")

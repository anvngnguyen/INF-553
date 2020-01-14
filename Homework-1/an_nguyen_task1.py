import sys
import json
from pyspark.sql import SparkSession


def main(input_file, output_file):
    result = {}

    spark = SparkSession.builder.master("local[*]").appName("HW1").getOrCreate()
    text = spark                                \
        .sparkContext                           \
        .textFile(input_file)                   \
        .coalesce(8)                            \
        .map(json.loads)                        \
        .map(lambda x: [x["user_id"],
                        x["name"],
                        x["review_count"],
                        x["yelping_since"]])    \
        .cache()

    # A. Find the total number of users
    result["total_users"] = text.count()

    # B. Find the average number of written reviews of all users
    total_reviews = text.map(lambda x: x[2]).reduce(lambda a, b: a + b)
    result["avg_reviews"] = total_reviews / result["total_users"]

    # C. Find the number of distinct user names
    names = text.map(lambda x: x[1]).groupBy(lambda x: x).cache()
    result["distinct_usernames"] = names.count()

    # D. Find the number of users that joined yelp in the year 2011
    result["num_users"] = text.map(lambda x: x[3]).filter(lambda x: "2011" in x).count()

    # E. Find Top 10 popular names and the number of times they appear
    result["top10_popular_names"] = names.mapValues(len).sortBy(lambda x: (-x[1], x[0])).take(10)

    # F. Find Top 10 user ids who have written the most number of reviews
    result["top10_most_reviews"] = text.map(lambda x: [x[0], x[2]]).sortBy(lambda x: (-x[1], x[0])).take(10)

    with open(output_file, "w") as f:
        json.dump(result, f)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

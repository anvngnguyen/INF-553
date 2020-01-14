import java.io.{File, PrintWriter}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object an_nguyen_task1 {
	implicit val formats: DefaultFormats.type = DefaultFormats

	case class User(user_id: String, name: String, review_count: Int, yelping_since: String, friends: Any,
					useful: Int, funny: Int, cool: Int, fans: Int, elite: Any, average_stars: Float,
					compliment_hot: Int, compliment_more: Int, compliment_profile: Int, compliment_cute: Int,
					compliment_list: Int, compliment_note: Int, compliment_plain: Int, compliment_cool: Int,
					compliment_funny: Int, compliment_writer: Int, compliment_photos: Int
				   )

	def main(args: Array[String]): Unit = {
		val pw = new PrintWriter(new File(args(1)))
		pw.write("{")

		Logger.getLogger("org").setLevel(Level.INFO)
		val spark = SparkSession
			.builder()
			.appName("an_nguyen_task1")
			.config("spark.master", "local[*]")
			.getOrCreate()

		val data = spark
			.sparkContext
			.textFile(args(0))
			.coalesce(8)
			.map(convertToJson)
			.map(x => (x.user_id, x.name, x.review_count, x.yelping_since))

		val total_users: Long = data.count()
		pw.write("\"total_users\": " + total_users + ",")

		val total_reviews = data.map(x => x._3).reduce((a, b) => (a + b))
		val avg_reviews = total_reviews.toString.toFloat / total_users.toFloat
		pw.write("\"avg_reviews\": " + avg_reviews + ",")

		val names = data.map(x => x._2).groupBy(x => x)
		pw.write("\"distinct_usernames\": " + names.count() + ",")

		val num_users = data.map(x => x._4).filter(x => x.contains("2011")).count()
		pw.write("\"num_users\": " + num_users + ",")

		pw.write("\"top10_popular_names\": [")
		val top_names = names.mapValues(x => x.size).sortBy(_._2, ascending = false).take(10)
		for (i <- top_names) {
			pw.write("[\"" + i._1 + "\", " + i._2 + "]")
			if (top_names.indexOf(i) < top_names.length - 1) {
				pw.write(",")
			}
		}
		pw.write("],")

		pw.write("\"top10_most_reviews\": [")
		val top_reviewers = data.map(x => (x._1, x._3)).sortBy(x => (-x._2, x._1)).take(10)
		for (i <- top_reviewers) {
			pw.write("[\"" + i._1 + "\", " + i._2 + "]")
			if (top_reviewers.indexOf(i) < top_reviewers.length - 1) {
				pw.write(",")
			}
		}
		pw.write("]")

		pw.write("}")
		pw.close()
	}

	def convertToJson(line: Any): User = {
		val temp = line + ""
		parse(temp).extract[User]
	}
}

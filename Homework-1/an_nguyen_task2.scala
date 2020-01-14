import java.io.{File, PrintWriter}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object an_nguyen_task2 {
	implicit val formats: DefaultFormats.type = DefaultFormats

	case class User(user_id: String, name: String, review_count: Int, yelping_since: String, friends: Any,
					useful: Int, funny: Int, cool: Int, fans: Int, elite: Any, average_stars: Float,
					compliment_hot: Int, compliment_more: Int, compliment_profile: Int, compliment_cute: Int,
					compliment_list: Int, compliment_note: Int, compliment_plain: Int, compliment_cool: Int,
					compliment_funny: Int, compliment_writer: Int, compliment_photos: Int
				   )

	def main(args: Array[String]): Unit = {
		Logger.getLogger("org").setLevel(Level.INFO)
		val spark = SparkSession
			.builder()
			.appName("an_nguyen_task1")
			.config("spark.master", "local[*]")
			.config("spark.executor.memory", "6g")
			.getOrCreate()
		val text = spark.sparkContext.textFile(args(0))

		val pw = new PrintWriter(new File(args(1)))
		pw.write("{")

		// Default Partition
		pw.write("\"default\": {")
		var start = System.currentTimeMillis()
		val default = text.coalesce(8)
		var temp = default
			.map(convertToJson)
			.map(x => (x.user_id, x.review_count))
			.map(x => (x._1, x._2)).sortBy(x => (-x._2, x._1), ascending = false).take(10)
		var size = default.glom().map(_.length).collect()
		var processTime = (System.currentTimeMillis() - start).toFloat / 1000.toFloat
		pw.write("\"n_partition\":" + default.getNumPartitions + ",\"n_items\":[")
		for (i <- size) {
			pw.write(i)
			if (size.indexOf(i) == size.length) {
				pw.write(",")
			}
		}
		pw.write("]")
		pw.write(",\"exe_time\":" + processTime)
		pw.write("},")

		// Customize Partition
		pw.write("\"customize\": {")
		start = System.currentTimeMillis()
		val customized = text.repartition(args(2).toInt).coalesce(8)
		temp = customized
			.map(convertToJson)
			.map(x => (x.user_id, x.review_count))
			.map(x => (x._1, x._2)).sortBy(x => (-x._2, x._1)).take(10)
		size = customized.glom().map(_.length).collect()
		processTime = (System.currentTimeMillis() - start).toFloat / 1000.toFloat
		pw.write("\"n_partition\":" + customized.getNumPartitions + ",\"n_items\":[")
		for (i <- size) {
			pw.write(i)
			if (size.indexOf(i) == size.length) {
				pw.write(",")
			}
		}
		pw.write("]")
		pw.write(",\"exe_time\":" + processTime)
		pw.write("},")

		pw.write("\"explanation\": \"As we can see, the size of each chunks in the Customized Partitioning is much " +
			"more uniformed comparing to its Default counterpart. That is because with Default Partitioning, data " +
			"is divided based on their location while in Customized, the data is divided equally.\"")

		pw.write("}")
		pw.close()
	}

	def convertToJson(line: Any): User = {
		val temp = line + ""
		parse(temp).extract[User]
	}
}

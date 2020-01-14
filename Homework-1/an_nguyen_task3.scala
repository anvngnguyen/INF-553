import java.io.{File, PrintWriter}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object an_nguyen_task3 {
	implicit val formats: DefaultFormats.type = DefaultFormats

	case class Business(business_id: String, name: String, address: String, city: String, state: String,
						postal_code: String, latitude: Float, longitude: Float, stars: Float, review_count: Int,
						is_open: Int, attributes: Any, categories: Any, hours: Any)

	case class Review(review_id: String, user_id: String, business_id: String, stars: Int, date: String, text: String,
					  useful: Int, funny: Int, cool: Int)

	def main(args: Array[String]): Unit = {
		Logger.getLogger("org").setLevel(Level.INFO)
		val spark = SparkSession
			.builder()
			.appName("an_nguyen_task1")
			.config("spark.master", "local[*]")
			.config("spark.executor.memory", "6g")
			.getOrCreate()
		val reviews = spark
			.sparkContext
			.textFile(args(0))
			.coalesce(8)
			.map(convertToReview)
			.map(x => (x.business_id, x.stars))
		val business = spark
			.sparkContext
			.textFile(args(1))
			.coalesce(8)
			.map(convertToBusinesss)
			.map(x => (x.business_id, x.state))
		val joinedData = reviews
			.join(business)
			.coalesce(8)
			.map(x => (x._2._2, x._2._1))
			.groupByKey()
			.map(x => (x._1, x._2.sum.toFloat / x._2.size.toFloat))
			.sortBy(x => (-x._2, x._1))

		// Task B
		// 	Method 1
		var pw = new PrintWriter(new File(args(3)))
		pw.write("{")
		pw.write("\"m1\":")
		var start = System.currentTimeMillis()
		val method_one_data = joinedData.collect()
		for (i <- method_one_data.take(5)) {
			println(i)
		}
		var processTime = (System.currentTimeMillis() - start).toFloat / 1000.toFloat
		pw.write(processTime + ",")
		// 	Method 2
		pw.write("\"m2\":")
		start = System.currentTimeMillis()
		val method_two_data = joinedData.take(5)
		for (i <- method_two_data) {
			println(i)
		}
		processTime = (System.currentTimeMillis() - start).toFloat / 1000.toFloat
		pw.write(processTime + ",")
		//	Explanation
		pw.write("\"explanation\": \"The first() method will be faster since it only return a portion of the RDD " +
			"(in this case is 5) comparing to the collect() which will return everything. Hence, Method B will " +
			"take a lot less time than Method A.\"}")
		pw.close()

		// Task A
		pw = new PrintWriter(new File(args(2)))
		pw.write("state,starts\n")
		for (i <- method_one_data) {
			pw.write(i._1 + "," + i._2 + "\n")
		}
		pw.close()
	}

	def convertToReview(line: Any): Review = {
		val temp = line + ""
		parse(temp).extract[Review]
	}

	def convertToBusinesss(line: Any): Business = {
		val temp = line + ""
		parse(temp).extract[Business]
	}
}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer


object an_nguyen_task1 {
	implicit val formats: DefaultFormats.type = DefaultFormats

	case class Business(business_id: String, name: String, neighborhood: String,
						address: String, city: String, state: String, postal_code: String,
						latitude: Double, longitude: Double, stars: Double, review_count: Int, is_open: Int,
						attributes: Any)

	val HASH_FUNCTIONS: Set[(Int, Int, Int)] = Set(
		(1, 16, 193), (11, 49, 1543), (19, 100, 12289), (37, 196, 196613),
		(3, 25, 389), (13, 64, 3079), (23, 121, 24593), (41, 225, 393241),
		(5, 36, 769), (17, 81, 6151), (31, 144, 98317), (43, 256, 786433),
		(1, 25, 769), (11, 64, 6151), (19, 121, 98317), (37, 225, 786433),
		(3, 36, 193), (13, 81, 1543), (23, 144, 12289), (41, 256, 196613),
		(5, 16, 389), (17, 49, 3079), (31, 100, 24593), (43, 196, 393241),
		(1, 36, 389), (11, 81, 3079), (19, 144, 24593), (37, 256, 393241),
		(3, 16, 769), (13, 49, 6151), (23, 100, 98317), (41, 196, 786433),
		(5, 25, 193), (17, 64, 1543), (31, 121, 12289), (43, 225, 196613)
	)
	val M = 200

	var filterBitArray: ListBuffer[Int] = ListBuffer.fill(M)(0)
	var seenStates: ListBuffer[String] = ListBuffer[String]()
	var falsePositive: Int = 0
	var trueNegative: Int = 0

	def main(args: Array[String]): Unit = {
		val port = args(0).toInt
		val outputFile = args(1)
		val pw = new PrintWriter(new File(outputFile))
		pw.write("Time,FPR")

		Logger.getLogger("org").setLevel(Level.WARN)
		val conf = new SparkConf().setMaster("local[*]").setAppName("Bloom")
		val ssc = new StreamingContext(conf, Seconds(10))

		val lines = ssc.socketTextStream("localhost", port)
		lines.foreachRDD((time, rdd) => bloomFiltering(time, rdd, pw))
		ssc.start()
		ssc.awaitTermination()
		pw.close()
	}

	def bloomFiltering(rdd: RDD[String], time: Time, pw: PrintWriter): Unit = {
		val receivedTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss"))
		val data = rdd.collect()
		for (i <- data) {
			val json_i = convertToJson(i)
			val state = json_i.state
			val stateCode = state.map(_.toByte).sum.abs
			val hashValues = ListBuffer[Int]()
			for (h <- HASH_FUNCTIONS)
				hashValues.append(((h._1 * stateCode + h._2) % h._3) % M)

			var counter = 0
			for (value <- hashValues)
				if (filterBitArray(value) == 1)
					counter += 1
			if (!seenStates.contains(state))
				if (counter == hashValues.length)
					falsePositive += 1
				else
					trueNegative += 1

			for (value <- hashValues)
				filterBitArray(value) = 1
			if (!seenStates.contains(state))
				seenStates.append(state)
		}

		val fpr = falsePositive.toFloat / (falsePositive.toFloat + trueNegative.toFloat)
		pw.write("\n" + receivedTime + "," + fpr)
	}

	def convertToJson(str: String): Business = {
		val temp = str + ""
		parse(temp).extract[Business]
	}
}

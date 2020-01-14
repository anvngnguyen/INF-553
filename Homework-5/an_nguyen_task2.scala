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


object an_nguyen_task2 {
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
	val NUM_GROUPS: Int = 4
	val ENTRIES_PER_GROUP: Int = HASH_FUNCTIONS.size / NUM_GROUPS
	val M = 200

	var pw: PrintWriter = _

	def main(args: Array[String]): Unit = {
		val port = args(0).toInt
		val outputFile = args(1)
		pw = new PrintWriter(new File(outputFile))
		pw.write("Time,FPR")

		Logger.getLogger("org").setLevel(Level.WARN)
		val conf = new SparkConf().setMaster("local[*]").setAppName("Bloom")
		val ssc = new StreamingContext(conf, Seconds(5))

		val lines = ssc.socketTextStream("localhost", port)
		lines.foreachRDD((time, rdd) => flajoletMartin(time, rdd, pw))
		ssc.start()
		ssc.awaitTermination()
		pw.close()
	}

	def flajoletMartin(rdd: RDD[String], time: Time, pw: PrintWriter): Unit = {
		val receivedTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss"))
		val actualDistinctCities = ListBuffer[String]()
		val hashBinaryBins = ListBuffer[ListBuffer[String]]()

		val data = rdd.collect()
		for (i <- data) {
			val json_i = convertToJson(i)
			val city = json_i.city
			val cityCode = city.map(_.toByte).sum.abs
			actualDistinctCities.append(city)
			val hashValues = ListBuffer[Int]()
			for (h <- HASH_FUNCTIONS)
				hashValues.append(((h._1 * cityCode + h._2) % h._3) % M)

			val hashBinaryValues = ListBuffer[String]()
			for (h <- HASH_FUNCTIONS) {
				val value = ((h._1 * cityCode + h._2) % h._3) % M
				val binaryValue = value.toBinaryString
				hashBinaryValues.append(binaryValue)
			}
			hashBinaryBins.append(hashBinaryValues)
		}

		val estimates = ListBuffer[Double]()
		for (i <- 0 until HASH_FUNCTIONS.size) {
			var r = 0
			for (binaryValues <- hashBinaryBins) {
				val trailingZeroes = countTrailingZeroes(binaryValues(i))
				if (trailingZeroes > r)
					r = trailingZeroes
			}
			estimates.append(math.pow(2, r))
		}

		val averageEstimates = ListBuffer[Double]()
		for (i <- 0 until  NUM_GROUPS) {
			var total = 0.0
			for (j <- 0 until ENTRIES_PER_GROUP)
				total += estimates(i * ENTRIES_PER_GROUP + j)
			averageEstimates.append(total / ENTRIES_PER_GROUP)
		}
		averageEstimates.toList.sorted
		val median = averageEstimates(NUM_GROUPS / 2)
		pw.write("\n" + receivedTime + "," + actualDistinctCities.length + "," + median)
	}

	def convertToJson(str: String): Business = {
		val temp = str + ""
		parse(temp).extract[Business]
	}

	def countTrailingZeroes(binaryString: String): Int = {
		binaryString.length - binaryString.replaceAll("0", "").length
	}
}

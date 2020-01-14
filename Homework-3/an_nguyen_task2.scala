import java.io.{File, PrintWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

object an_nguyen_task2 {
	def main(args: Array[String]): Unit = {
		val start = System.nanoTime()
		val trainFile = args(0)
		val testFile = args(1)
		val caseID = args(2)
		val outputPath = args(3)
		Logger.getLogger("org").setLevel(Level.WARN)
		val spark = SparkSession.builder.master("local[*]").appName("HW3").getOrCreate()

		val processedData = processData(spark, trainFile, testFile)
		var predictions: RDD[((Int, Int), Double)] = null
		if (caseID == "1")
			predictions = modelBasedCF(processedData._3, processedData._5, processedData._4)
		else if (caseID == "2")
			predictions = userBasedCF(processedData._3, processedData._5)
		else {
			print("Invalid Case Id")
			return
		}
		outputToFile(outputPath, processedData._1, processedData._2, predictions)

		spark.close()
		val end = System.nanoTime()
		val time = (end - start) / 1000000000
		println("Execution Time: " + time + " sec.")
	}

	def processData(spark: SparkSession, trainFile: String, testFile: String): (List[String], List[String], RDD[Rating], RDD[Rating], RDD[(Int, Int)]) = {
		// 1. Process training data
		val trainText = spark.sparkContext.textFile(trainFile)
		var header = trainText.first()
		val trainRawData = trainText.filter(row => row != header).map(x => x.split(","))
		val trainUsers = trainRawData.map(x => x(0)).distinct().collect().toList
		val trainBusinesses = trainRawData.map(x => x(1)).distinct().collect().toList

		// 2. Process testing data
		val testText = spark.sparkContext.textFile(testFile)
		header = testText.first()
		val testRawData = testText.filter(row => row != header).map(x => x.split(","))
		val testUsers = testRawData.map(x => x(0)).distinct().collect().toList
		val testBusinesses = testRawData.map(x => x(1)).distinct().collect().toList

		// 3. Create User/Business Dictionary and convert User/Business ID from String to Integer
		val users = trainUsers ::: testUsers
		val usersMap = users.map(x => x -> users.indexOf(x)).toMap
		val businesses = trainBusinesses ::: testBusinesses
		val businessesMap = businesses.map(x => x -> businesses.indexOf(x)).toMap
		val trainData = trainRawData.map(x => Rating(usersMap(x(0)), businessesMap(x(1)), x(2).toDouble))
		val testData = testRawData.map(x => Rating(usersMap(x(0)), businessesMap(x(1)), x(2).toDouble))

		// 4. Create Test Keys for Predict Step
		val testDataKeys = testData.map(x => (x.user, x.product))

		(users, businesses, trainData, testData, testDataKeys)
	}

	def adjustPrediction(x: Rating): ((Int, Int), Double) = {
		if (x.rating > 5)
			((x.user, x.product), 5)
		else if (x.rating < 1)
			((x.user, x.product), 1)
		else
			((x.user, x.product), x.rating)
	}

	def modelBasedCF(trainData: RDD[Rating], testDataKeys: RDD[(Int, Int)], testData: RDD[Rating]): RDD[((Int, Int), Double)] = {
		val als = new ALS()
		als.setRank(2)
		als.setIterations(5)
		als.setLambda(0.25)
		als.setNonnegative(true)
		val model = als.run(trainData)
		var predictions = model.predict(testDataKeys).map(adjustPrediction)
		val noPredictions = testData
			.map(x => ((x.user, x.product), None))
			.map(x => (x._1, None))
			.subtractByKey(predictions)
			.map(x => (x._1, 3.0))
		predictions = predictions.union(noPredictions)
		predictions
	}

	def pearsonCorrelation(pairsRating: ListBuffer[(Double, Double)]): Double = {
		if (pairsRating.isEmpty)
			0
		else {
			val ratingOne: ListBuffer[Double] = ListBuffer.empty[Double]
			val ratingTwo: ListBuffer[Double] = ListBuffer.empty[Double]
			for (r <- pairsRating) {
				ratingOne.append(r._1)
				ratingTwo.append(r._2)
			}
			val averageOne = ratingOne.sum / ratingOne.length
			val averageTwo = ratingTwo.sum / ratingTwo.length
			val differenceOne: ListBuffer[Double] = ListBuffer.empty[Double]
			for (r <- ratingOne)
				differenceOne.append(r - averageOne)
			val differenceTwo: ListBuffer[Double] = ListBuffer.empty[Double]
			for (r <- ratingTwo)
				differenceTwo.append(r - averageTwo)
			var numerator = 0.0
			for ((i, j) <- differenceOne zip differenceTwo)
				numerator += i * j
			var denominatorOne = 0.0
			var denominatorTwo = 0.0
			for ((i, j) <- differenceOne zip differenceTwo) {
				denominatorOne += math.pow(i, 2)
				denominatorTwo += math.pow(j, 2)
			}
			val denominator = denominatorOne * denominatorTwo
			if (denominator == 0)
				0
			else
				numerator / denominator
		}
	}

	def userBasedPredict(x: (Int, Int), allUsersAllRatingsMap: Map[Int, Map[Int, Double]], allUsersAvgRatingMap: Map[Int, Double], allBusinesseseAllRatingsMap: Map[Int, Map[Int, Double]]): Rating = {
		val user = x._1
		val business = x._2
		var r_a: Double = 3
		if (allUsersAvgRatingMap.contains(user))
			r_a = allUsersAvgRatingMap(user)
		var userAllRating: Map[Int, Double] = Map.empty[Int, Double]
		if (allUsersAllRatingsMap.contains(user))
			userAllRating = allUsersAllRatingsMap(user)
		var businessRating: Map[Int, Double] = Map.empty[Int, Double]
		if (allBusinesseseAllRatingsMap.contains(business))
			businessRating = allBusinesseseAllRatingsMap(business)

		var total = 0.0
		val weights: ListBuffer[Double] = ListBuffer.empty[Double]
		for (rating <- businessRating) {
			val neighbor = rating._1
			val neighborRating = rating._2
			var neighborAllRating: Map[Int, Double] = Map.empty[Int, Double]
			if (allUsersAllRatingsMap.contains(user))
				neighborAllRating = allUsersAllRatingsMap(neighbor)
			val pairsRating: ListBuffer[(Double, Double)] = ListBuffer.empty[(Double, Double)]
			for (b <- userAllRating.keys)
				if (neighborAllRating.contains(b))
					pairsRating.append((userAllRating(b), neighborAllRating(b)))
			val correlation = pearsonCorrelation(pairsRating)
			total += (neighborRating - allUsersAvgRatingMap(neighbor)) * correlation
			weights.append(math.abs(correlation))
		}
		val weightSum = weights.sum
		if (weightSum == 0)
			Rating(user, business, r_a)
		else
			Rating(user, business, r_a + total / weightSum)
	}

	def userBasedCF(trainData: RDD[Rating], testDataKeys: RDD[(Int, Int)]): RDD[((Int, Int), Double)] = {
		val allUsersAvgRating = trainData.map(x => (x.user, x.rating)).groupByKey().map(x => (x._1, x._2.sum / x._2.size))
		val allUsersAvgRatingMap = allUsersAvgRating.collect().toMap
		val allBusinessesAllRatings = trainData.map(x => (x.product, (x.user, x.rating))).groupByKey().map(x => (x._1, x._2.toMap))
		val allBusinessesAllRatingsMap = allBusinessesAllRatings.collect().toMap
		val allUsersAllRatings = trainData.map(x => (x.user, (x.product, x.rating))).groupByKey().map(x => (x._1, x._2.toMap))
		val allUsersAllRatingsMap = allUsersAllRatings.collect().toMap
		val predictions = testDataKeys.map(x => userBasedPredict(x, allUsersAllRatingsMap, allUsersAvgRatingMap, allBusinessesAllRatingsMap)).map(adjustPrediction)
		predictions
	}

	def outputToFile(outputPath: String, users: List[String], businesses: List[String], predictions: RDD[((Int, Int), Double)]) {
		val finalResult = predictions.map(x => (users(x._1._1), businesses(x._1._2), x._2)).collect()
		val pw = new PrintWriter(new File(outputPath))
		pw.write("user_id, business_id, prediction\n")
		for (c <- finalResult)
			pw.write(c._1 + "," + c._2 + "," + c._3 + "\n")
		pw.close()
	}
}

import java.io.{File, PrintWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

object an_nguyen_task1 {
	val HASH_VALUES = Array(
		(1, 16, 193), (11, 49, 1543), (19, 100, 12289), (37, 196, 196613),
		(3, 25, 389), (13, 64, 3079), (23, 121, 24593), (41, 225, 393241),
		(5, 36, 769), (17, 81, 6151), (31, 144, 98317), (43, 256, 786433),
		(1, 25, 769), (11, 64, 6151), (10, 121, 98317), (37, 225, 786433),
		(3, 36, 193), (13, 81, 1543), (23, 144, 12289), (41, 256, 196613),
		(5, 16, 389), (17, 49, 3079), (31, 100, 24593), (43, 196, 393241),
		(1, 36, 389), (11, 81, 3079), (10, 144, 24593), (37, 256, 393241),
		(3, 16, 769), (13, 49, 6151), (23, 100, 98317), (41, 196, 786433),
		(5, 25, 193), (17, 64, 1543), (31, 121, 12289), (43, 225, 196613)
	)
	val N: Int = HASH_VALUES.length
	val BANDS: Int = 18
	val ROWS: Int = N / BANDS

	def minHash(ints: Iterable[Int], binCount: Int): Iterable[Int] = {
		var signature = ArrayBuffer[Int]()
		for (h <- HASH_VALUES) {
			var values = ArrayBuffer[Int]()
			for (i <- ints) {
				values += ((h._1 * i + h._2) % h._3) % binCount
			}
			signature += values.min
		}
		signature
	}

	def createCandidateGroup(x: (String, Iterable[Int])): List[((Int, Tuple1[Iterable[Int]]), String)] = {
		val bins = scala.collection.mutable.ListBuffer.empty[((Int, Tuple1[Iterable[Int]]), String)]
		for (i <- 0 until BANDS) {
			bins.append(Tuple2(Tuple2(i, Tuple1(x._2.slice(i * ROWS, (i + 1) * ROWS))), x._1))
		}
		bins.toList
	}

	def createCandidatePairs(x: ((Int, Tuple1[Iterable[Int]]), Iterable[String])): Iterator[Set[String]] = {
		x._2.toSet.subsets(2)
	}

	def jaccardSimilarity(pairs: Set[String], inputMatrix: Map[String, Iterable[Int]]): (String, String, Double) = {
		val temp = pairs.toArray.sorted
		val a = inputMatrix(temp(0)).toSet
		val b = inputMatrix(temp(1)).toSet
		val intersection = a.intersect(b)
		val union = a.union(b)
		(temp(0), temp(1), intersection.size.toDouble / union.size.toDouble)
	}

	def jaccardLSH(data: RDD[Array[String]]): RDD[(String, String, Double)] = {
		// 1. Process data
		val users = data.map(x => x(0)).distinct().collect()
		val usersMap = users.toList.map(x => x -> users.indexOf(x)).toMap
		val inputMatrix = data.map(x => (x(1), usersMap(x(0)))).groupByKey().sortBy(x => x._1)
		val inputMatrixMap = inputMatrix.collect().toMap

		// 2. Calculate minimum hashing signatures for each product
		val binCount = users.length / N
		val signatures = inputMatrix.map(x => (x._1, minHash(x._2, binCount)))

		// 3. Divide the Matrix into b bands with r rows each and generate the candidate pairs
		val groups = signatures.flatMap(createCandidateGroup).groupByKey().filter(x => x._2.size > 1)
		val pairs = groups.flatMap(createCandidatePairs).distinct()

		// 4. Calculate Jaccard Similarity to get the result
		val result = pairs.map(x => jaccardSimilarity(x, inputMatrixMap)).filter(x => x._3 >= 0.5)
		result
	}

	// TODO: Implement this if I ever figure out how to use the package
	def cosineLSH(data: RDD[Array[String]]): RDD[(String, String, Double)] = {
		val result: RDD[(String, String, Double)] = null
		result
	}

	def outputToFile(outputPath: String, result: Array[(String, String, Double)]) {
		val pw = new PrintWriter(new File(outputPath))
		pw.write("business_id_1, business_id_2, similarity\n")
		for (c <- result) {
			pw.write(c._1 + "," + c._2 + "," + c._3 + "\n")
		}
		pw.close()
	}


	def main(args: Array[String]): Unit = {
		val start = System.nanoTime()
		Logger.getLogger("org").setLevel(Level.WARN)
		val spark = SparkSession.builder.master("local[*]").appName("HW3").getOrCreate()
		val inputFile = args(0)
		val similarityMethod = args(1)
		val outputPath = args(2)

		val text = spark.sparkContext.textFile(inputFile)
		val head = text.first()
		val data = text.filter(row => row != head).map(x => x.split(","))

		var result: RDD[(String, String, Double)] = null
		if (similarityMethod == "jaccard")
			result = jaccardLSH(data)
		else if (similarityMethod == "cosine")
			result = cosineLSH(data)
		else {
			println("Invalid Similarity Method!")
			return
		}

		val finalResult = result.sortBy(x => (x._1, x._2)).collect()
		outputToFile(outputPath, finalResult)
		spark.close()

		val end = System.nanoTime()
		val time = (end - start) / 1000000000
		println("Duration: " + time)
	}
}

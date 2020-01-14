import java.io.{File, PrintWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import Ordering.Implicits._

object an_nguyen_task2 {
	def main(args: Array[String]): Unit = {
		val start = System.currentTimeMillis()

		val filterThreshold = args(0).toInt
		val minCount = args(1).toInt
		val inputFile = args(2)
		val outputFile = args(3)
		Logger.getLogger("org").setLevel(Level.INFO)
		val spark = SparkSession.builder().appName("task2").config("spark.master", "local[*]").getOrCreate()
		val data = processData(spark, inputFile)

		var baskets: RDD[List[Long]] = null
		baskets = data.map(line => (line.split(',')(0), line.split(',')(1).toLong)).groupByKey().map(_._2.toList).filter(x => x.size > filterThreshold)

		val phase1 = baskets
			.mapPartitions(chunk => aPriori(chunk, minCount, baskets.getNumPartitions))
			.reduceByKey((a, b) => a.union(b))
		val candidate = phase1.collect()

		val pw = new PrintWriter(new File(outputFile))
		pw.write("Candidates:")
		var previousLength = 0
		for (c <- candidate.sortBy(x => x._1)) {
			for (i <- c._2.toList.sortBy(x => x.toList)) {
				if (previousLength != i.size) {
					previousLength = i.size
					pw.write("\n")
					if (previousLength > 1)
						pw.write("\n")
				}
				else
					pw.write(",")
				var k = 1
				pw.write("(")
				for (j <- i) {
					if (i.size == 1)
						pw.write("'" + j + "')")
					else {
						pw.write("'" + j + "'")
						if (k == i.size)
							pw.write(")")
						else if (i.size > 1 && k < i.size)
							pw.write(", ")
					}
					k += 1
				}
			}
		}
		pw.write("\n")
		pw.write("\n")

		val phase2 = baskets.flatMap(chunk => {
			val basket = chunk.toSet
			var occurrences = mutable.Map.empty[Set[Long], Int]
			for (c <- candidate)
				for (i <- c._2)
					if (i.subsetOf(basket)) {
						if (!occurrences.contains(i))
							occurrences += (i -> 0)
						occurrences(i) += 1
					}
			occurrences
		})

		val freqItems = phase2.reduceByKey(_ + _).filter(_._2 >= minCount).collect()
		previousLength = 0
		pw.write("Frequent Itemsets:")
		for (i <- freqItems.sortBy(x => (x._1.size, x._1.toList))) {
			if (previousLength != i._1.size) {
				previousLength = i._1.size
				pw.write("\n")
				if (previousLength > 1)
					pw.write("\n")
			}
			else
				pw.write(",")
			var k = 1
			pw.write("(")
			for (j <- i._1) {
				if (i._1.size == 1)
					pw.write("'" + j + "')")
				else {
					pw.write("'" + j + "'")
					if (k == i._1.size)
						pw.write(")")
					else if (i._1.size > 1 && k < i._1.size)
						pw.write(", ")
				}
				k += 1
			}
		}
		pw.write("\n")
		pw.close()

		spark.stop()
		val processTime = (System.currentTimeMillis() - start).toFloat / 1000.toFloat
		println("Duration: " + processTime)
	}

	def processData(spark: SparkSession, inputFile: String): RDD[String] = {
		val raw_text = spark.sparkContext.textFile(inputFile)
		val text = raw_text.map(x => x.split(","))
		val head = text.first()
		val raw_data = text.filter(row => !(row sameElements head))
		raw_data.map(x => (x(0) + "-" + x(1)).replace("\"", "") + "," + x(5).replace("\"", ""))
	}

	def aPriori(chunk: Iterator[List[Long]], minCount: Int, numPartitions: Int): Iterator[(Int, mutable.Set[Set[Long]])] = {
		val minSupport = minCount.toFloat / numPartitions.toFloat

		var occurrences = mutable.Map.empty[Long, Int]
		var baskets = new mutable.ListBuffer[Set[Long]]
		while (chunk.hasNext) {
			val basket = chunk.next()
			baskets += basket.toSet
			for (item <- basket) {
				if (!occurrences.contains(item))
					occurrences += (item -> 0)
				occurrences(item) += 1
			}
		}

		val freqItemsMap = mutable.Map.empty[Int, mutable.Set[Set[Long]]]
		var freqItems = mutable.Set.empty[Set[Long]]
		for (o <- occurrences)
			if (o._2 >= minSupport)
				freqItems += Set(o._1)
		freqItemsMap += (1 -> freqItems)

		var k = 2
		while (freqItemsMap.contains(k - 1) && freqItemsMap(k - 1).nonEmpty) {
			var new_candidates = mutable.Set.empty[Set[Long]]
			for (i <- freqItemsMap(k - 1)) {
				for (j <- freqItemsMap(k - 1)) {
					val candidate = i.union(j).toList.sorted.toSet
					if (candidate.size == k)
						new_candidates += candidate
				}
			}

			var occurrences = mutable.Map.empty[Set[Long], Int]
			if (new_candidates.nonEmpty) {
				for (candidate <- new_candidates)
					for (b <- baskets)
						if (candidate.subsetOf(b)) {
							if (!occurrences.contains(candidate))
								occurrences += (candidate -> 0)
							occurrences(candidate) += 1
						}
			}

			freqItems = mutable.Set.empty[Set[Long]]
			for (o <- occurrences)
				if (o._2 >= minSupport)
					freqItems += o._1

			if (freqItems.nonEmpty)
				freqItemsMap += (k -> freqItems)
			k += 1
		}

		freqItemsMap.toIterator
	}
}

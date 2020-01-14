import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.io.{File, PrintWriter}


object an_nguyen_task2 {
	def main(args: Array[String]): Unit = {
		val inputFile = args(0)
		val betweennessOutputFilePath = args(1)
		val communityOutputFilePath = args(2)
		Logger.getLogger("org").setLevel(Level.ERROR)
		val spark = SparkSession.builder.master("local[*]").appName("HW3").getOrCreate()

		val sc = spark.sparkContext
		val textRDD = sc.textFile(inputFile).map(x => x.split(" ")).map(x => (x(0), x(1)))
		val edges = textRDD.collect()
		val graph = mutable.Map[String, mutable.Set[String]]()
		for (pair <- edges) {
			if (!graph.contains(pair._1))
				graph += (pair._1 -> mutable.Set[String]())
			graph(pair._1).add(pair._2)
			if (!graph.contains(pair._2))
				graph += (pair._2 -> mutable.Set[String]())
			graph(pair._2).add(pair._1)
		}
		val vertices = mutable.Set[String]()
		for (i <- textRDD.map(x => x._1).collect())
			vertices += i
		for (j <- textRDD.map(x => x._2).collect())
			vertices += j

		// Calculate Betweenness
		val betweenness = calculateBetweenness(vertices, graph)
		val betweennessResult = betweenness.toList.map(x => (x._1._1, x._1._2, x._2)).sortBy(x => (-x._3, x._1, x._2))
		var output = betweennessOutputFilePath
		if (!betweennessOutputFilePath.contains("an_nguyen_task2_edge_betweenness_scala.txt"))
			output += "/an_nguyen_task2_edge_betweenness_scala.txt"
		var pw = new PrintWriter(new File(output))
		for (b <- betweennessResult)
			pw.write("('" + b._1 + "', '" + b._2 + "'), " + b._3 + "\n")
		pw.close()

		// Community Detection
		var result = communityDetection(vertices, graph, betweenness)
		result = result.sortBy(x => (x.length, x.head))
		output = betweennessOutputFilePath
		if (!communityOutputFilePath.contains("an_nguyen_task2_community_scala.txt"))
			output += "/an_nguyen_task2_community_scala.txt"
		pw = new PrintWriter(new File(output))
		for (c <- result)
			for (i <- c) {
				pw.write("'" + i + "'")
				if (c.indexOf(i) == c.length - 1)
					pw.write("\n")
				else
					pw.write(",")
			}
		pw.close()
	}

	def calculateBetweenness(vertices: mutable.Set[String], graph: mutable.Map[String, mutable.Set[String]]): mutable.Map[(String, String), Double] = {
		val betweenness = mutable.Map[(String, String), Double]()
		for (vertex <- vertices) {
			val bfs_result = bfs(vertex, graph)
			val edgeCredit = calculateEdgeCredit(bfs_result._1, bfs_result._2, bfs_result._3)
			for (edge <- edgeCredit.keys) {
				if (!betweenness.contains(edge))
					betweenness(edge) = 0
				betweenness(edge) += edgeCredit(edge) / 2
			}
		}
		betweenness
	}

	def bfs(root: String, graph: mutable.Map[String, mutable.Set[String]]): (ListBuffer[String], mutable.Map[String, ListBuffer[String]], mutable.Map[String, Int]) = {
		val queue = ListBuffer[String]()
		queue.append(root)
		val visited = ListBuffer[String]()
		visited.append(root)
		val level = mutable.Map[String, Int]()
		val parents = mutable.Map[String, ListBuffer[String]]()
		val shortestPath = mutable.Map[String, Int]()

		for (vertex <- graph.keys) {
			level(vertex) = -1
			parents(vertex) = ListBuffer[String]()
			shortestPath(vertex) = 0
		}
		shortestPath(root) = 1
		level(root) = 0

		while (queue.nonEmpty) {
			val current = queue.remove(0)
			visited.append(current)
			val children = graph(current)
			for (child <- children) {
				if (level(child) == -1) {
					queue.append(child)
					level(child) = level(current) + 1
				}
				if (level(child) == level(current) + 1) {
					parents(child).append(current)
					shortestPath(child) = shortestPath(child) + shortestPath(current)
				}
			}
		}
		(visited, parents, shortestPath)
	}

	def calculateEdgeCredit(visited: ListBuffer[String], parents: mutable.Map[String, ListBuffer[String]], shortestPath: mutable.Map[String, Int]): mutable.Map[(String, String), Double] = {
		val edgeCredit = mutable.Map[(String, String), Double]()
		val vertexCredit = mutable.Map[String, Double]()
		for (vertex <- visited)
			vertexCredit(vertex) = 1
		for (vertex <- visited.reverse)
			for (parent <- parents(vertex)) {
				val credit = vertexCredit(vertex) * shortestPath(parent) / shortestPath(vertex)
				val pair = ListBuffer[String]()
				pair.append(vertex)
				pair.append(parent)
				val orderedPair = Tuple2(pair.min, pair.max)
				if (!edgeCredit.contains(orderedPair))
					edgeCredit(orderedPair) = 0.0
				edgeCredit(orderedPair) += credit
				vertexCredit(parent) += credit
			}
		edgeCredit
	}

	def communityDetection(vertices: mutable.Set[String], graph: mutable.Map[String, mutable.Set[String]], betweenness: mutable.Map[(String, String), Double]): ListBuffer[ListBuffer[String]] = {
		val numberOfEdges = betweenness.size
		var btw = mutable.Map[(String, String), Double]() ++ betweenness
		val newGraph = mutable.Map[String, mutable.Set[String]]() ++ graph

		var maxModularity = -1.0
		var maxCommunities = ListBuffer[ListBuffer[String]]()
		while (btw.nonEmpty) {
			val communities = createCommunities(newGraph, vertices)
			val modularity = calculateModularity(communities, graph, numberOfEdges)
			if (modularity > maxModularity) {
				maxModularity = modularity
				maxCommunities = communities
			}
			val maxBetweenness: Double = btw.values.max
			var removedEdges = ListBuffer[(String, String)]()
			for (i <- btw)
				if (i._2 == maxBetweenness)
					removedEdges += i._1
			for (edge <- removedEdges) {
				newGraph(edge._1) = newGraph(edge._1).filter(_ != edge._2)
				newGraph(edge._2) = newGraph(edge._2).filter(_ != edge._1)
			}
			btw = calculateBetweenness(vertices, newGraph)
		}
		maxCommunities
	}

	def createCommunities(graph: mutable.Map[String, mutable.Set[String]], vertices: mutable.Set[String]): ListBuffer[ListBuffer[String]] = {
		var availableVertices = mutable.Set[String]()
		for (vertex <- vertices)
			availableVertices += vertex
		val communities = ListBuffer[ListBuffer[String]]()
		while (availableVertices.nonEmpty) {
			val community = createCommunity(graph, availableVertices.toList.head)
			val communityListBuffer = ListBuffer[String]()
			for (i <- community)
				communityListBuffer += i
			communities.append(communityListBuffer.sorted)
			availableVertices = availableVertices.diff(community)
		}
		communities
	}

	def createCommunity(graph: mutable.Map[String, mutable.Set[String]], vertex: String): mutable.Set[String] = {
		val community = mutable.Set[String]()
		val queue = ListBuffer[String]()
		queue.append(vertex)
		while (queue.nonEmpty) {
			val current = queue.remove(0)
			community.add(current)
			for (neighbor <- graph(current))
				if (!community.contains(neighbor))
					queue.append(neighbor)
		}
		community
	}

	def calculateModularity(communities: ListBuffer[ListBuffer[String]], graph: mutable.Map[String, mutable.Set[String]], numberOfEdges: Int): Double = {
		var totalModularity = 0.0
		for (community <- communities)
			for (i <- community)
				for (j <- community) {
					val k_i = graph(i).size
					val k_j = graph(j).size
					var a_ij = 0.0
					if (graph(i).contains(j))
						a_ij = 1
					totalModularity += a_ij - k_i * k_j / (2 * numberOfEdges.toFloat)
				}
		totalModularity / (2 * numberOfEdges.toFloat)
	}
}

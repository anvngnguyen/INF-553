import sys
import time
import operator
from pyspark.sql import SparkSession


def bfs(root, graph):
    queue = [root]
    visited = [root]
    level = {}
    parents = {}
    shortest_path = {}

    for vertex in graph.keys():
        level[vertex] = -1
        parents[vertex] = []
        shortest_path[vertex] = 0
    shortest_path[root] = 1
    level[root] = 0

    while queue:
        current = queue.pop(0)
        visited.append(current)
        children = graph[current]
        for child in children:
            if level[child] == -1:
                queue.append(child)
                level[child] = level[current] + 1
            if level[child] == level[current] + 1:
                parents[child].append(current)
                shortest_path[child] = shortest_path[child] + shortest_path[current]

    return visited, parents, shortest_path


def calculate_edge_credit(visited, parents, shortest_path):
    edge_credit = {}
    vertex_credit = {vertex: 1 for vertex in visited}
    for vertex in visited[::-1]:
        for parent in parents[vertex]:
            pair = min(vertex, parent), max(vertex, parent)
            credit = vertex_credit[vertex] * shortest_path[parent] / shortest_path[vertex]
            if pair not in edge_credit.keys():
                edge_credit[pair] = 0
            edge_credit[pair] += credit
            vertex_credit[parent] += credit
    return edge_credit


def calculate_betweenness(vertices, graph):
    betweenness = {}
    for vertex in vertices:
        visited, parents, shortest_path = bfs(vertex, graph)
        edge_credit = calculate_edge_credit(visited, parents, shortest_path)
        for edge, credit in edge_credit.items():
            if edge not in betweenness.keys():
                betweenness[edge] = 0
            betweenness[edge] += credit / 2
    return sorted(betweenness.items(), key=operator.itemgetter(1), reverse=True)


def create_community(graph, vertex):
    community = set()
    queue = [vertex]
    while queue:
        current = queue.pop(0)
        community.add(current)
        for neighbor in graph[current]:
            if neighbor not in community:
                queue.append(neighbor)
    return community


def create_communities(graph, vertices):
    temp = vertices.copy()
    communities = []
    while temp:
        community = create_community(graph, temp.pop())
        communities.append(sorted(list(community)))
        temp = temp.difference(community)
    return communities


def calculate_modularity(communities, graph, number_of_edges):
    total_modularity = 0.0
    for community in communities:
        for i in community:
            for j in community:
                k_i = len(graph[i])
                k_j = len(graph[j])
                a_ij = 1.0 if j in graph[i] else 0.0
                total_modularity += a_ij - k_i * k_j / (2 * number_of_edges)
    return total_modularity / (2 * number_of_edges)


def community_detection(vertices, graph, betweenness):
    number_of_edges = len(betweenness)
    btw, new_graph = betweenness.copy(), graph.copy()
    max_modularity, max_communities = -1, []
    while btw:
        communities = create_communities(new_graph, vertices)
        modularity = calculate_modularity(communities, graph, number_of_edges)
        if max_modularity < modularity:
            max_communities = communities
            max_modularity = modularity
        max_betweenness = max([v for _, v in btw])
        removed_edges = [edge for edge, value in btw if value == max_betweenness]
        for edge in removed_edges:
            new_graph[edge[0]] = {i for i in new_graph[edge[0]] if i != edge[1]}
            new_graph[edge[1]] = {i for i in new_graph[edge[1]] if i != edge[0]}
        btw = calculate_betweenness(vertices, new_graph)
    return max_communities


def main():
    start = time.time()

    input_filepath, betweenness_output_filepath, community_output_filepath = sys.argv[1:]
    spark = SparkSession.builder.master("local[*]").appName("HW4-Task2").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    text_rdd = sc.textFile(input_filepath).map(lambda x: x.split(" ")).map(lambda x: (x[0], x[1]))
    edges = text_rdd.collect()
    graph = {}
    for pair in edges:
        if pair[0] not in graph.keys():
            graph[pair[0]] = set()
        graph[pair[0]].add(pair[1])
        if pair[1] not in graph.keys():
            graph[pair[1]] = set()
        graph[pair[1]].add(pair[0])
    vertices = set(text_rdd.map(lambda x: x[0]).collect() + text_rdd.map(lambda x: x[1]).collect())

    # Calculate Betweenness
    betweenness = calculate_betweenness(vertices, graph)
    with open(betweenness_output_filepath, "w") as f:
        for i in betweenness:
            f.write(f"{i[0]}, {i[1]}\n")

    # Community Detection
    result = community_detection(vertices, graph, betweenness)
    result.sort(key=lambda x: (len(x), x[0]))
    with open(community_output_filepath, "w") as f:
        for i in result:
            f.write("'" + "','".join(i) + "'\n")

    spark.stop()
    print(f"Duration: {time.time() - start}")


main()

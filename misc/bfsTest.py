#!/usr/bin/env python

# title           : BFS.py
# description     : Breadth first search test over dictionary
# author		  : Ajay Krishna Teja Kavuri
# date            : 20161105
# version         : 1.0
# ==============================================================================

# graph is in adjacent list representation
# graph = {'A': {'B':'B', 'C':'C'},
#          'B': {'A':'A', 'D':'D', 'E':'E'},
#          'C': {'A':'A', 'F':'F'},
#          'D': {'B':'B'},
#          'E': {'B':'B', 'F':'F'},
#          'F': {'C':'C', 'E':'E'}}
#
# def bfs(graph, start):
#     visited, queue = {}, [start]
#     while queue:
#         vertex = queue.pop(0)
#         if not visited.get(vertex):
#             visited[vertex]=vertex
#             queue.extend(graph[vertex] - visited)
#     return visited
#
# print bfs(graph, 'A') # {'B', 'C', 'A', 'F', 'D', 'E'}
# graph = {'A': set(['B', 'C']),
#          'B': set(['A', 'D', 'E']),
#          'C': set(['A', 'F']),
#          'D': set(['B']),
#          'E': set(['B', 'F']),
#          'F': set(['C', 'E'])}

graph = {'A': {'B':'B', 'C':'C'},
         'B': {'A':'A', 'D':'D', 'E':'E'},
         'C': {'A':'A', 'F':'F'},
         'D': {'B':'B'},
         'E': {'B':'B', 'F':'F'},
         'F': {'C':'C', 'E':'E'}}

def bfs_paths(graph, start, goal):
    queue = [(start, [start])]
    while queue:
        (vertex, path) = queue.pop(0)

        curSet = set()
        curDict = graph.get(vertex)

        for node in curDict.keys():
            curSet.add(node)
        for next in curSet - set(path):
            if next == goal:
                yield path + [next]
            else:
                queue.append((next, path + [next]))

def shortest_path(graph, start, goal):
    try:
        return next(bfs_paths(graph, start, goal))
    except StopIteration:
        return None

print shortest_path(graph, 'A', 'D') # ['A', 'C', 'F']

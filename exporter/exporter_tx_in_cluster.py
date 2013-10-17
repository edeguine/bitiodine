#!/usr/bin/env python3
import networkx as nx
import pickle
import gc

from sys import argv

# Config
# Cluster number
cluster_n = int(argv[1])

with open("../clusterizer/clusters.dat", "rb") as cf:
	users = pickle.load(cf)

print("Clusters loaded.")

addresses = set()
for address, cluster in users.items():
	if cluster == cluster_n:
		addresses.add(address)
print("%d addresses loaded." % len(addresses))
del users

gc.collect()

with open('../grapher/tx_graph.dat', "rb") as infile:
	G = pickle.load(infile)

print("Graph loaded.")

nodes, edges = set(), []

with open(str(cluster_n) + ".dot", 'w') as f:
	f.write('digraph G {\n');

	for u, v in G.edges_iter():
		#if (u not in addresses or v not in addresses):
		if (u not in addresses and v not in addresses):
			continue

		nodes.add(u)
		nodes.add(v)
		edges.append((u, v))

	print("Filtering results: %d nodes and %d edges." % (len(nodes), len(edges)))
	print("Generating a DOT file...")

	nodes = sorted(list(nodes))

	for n in nodes:
		f.write('"%s";\n' % (n))

	f.write('\n')
	f.flush()

	for edge in edges:
		(u, v) = edge
		f.write('"%s" -> "%s";\n' % (u, v)))

	f.write('};\n')
	f.flush()
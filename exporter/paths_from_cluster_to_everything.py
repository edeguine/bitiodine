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

paths = []

with open(str(cluster_n) + "_to_" + str(dest_addr) + ".txt", 'w') as f:

	for address in addresses:
		new_paths = list(nx.all_simple_paths(G, source=address))
		paths += new_paths

		print("Added %d new paths from address %s with min length %d." %(len(new_paths), address, min([len(x) for x in new_paths])))

	# Sort paths by length
	paths.sort(key=len)

	for path in paths:
		f.write("%s\n" % '->'.join(path))
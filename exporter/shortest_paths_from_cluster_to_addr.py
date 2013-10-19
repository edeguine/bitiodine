#!/usr/bin/env python3
import networkx as nx
import pickle
import gc
from multiprocessing import Pool

from sys import argv

# Config
# Cluster number
cluster_n = int(argv[1])
dest_addr = argv[2]

def find(address):
	path = nx.shortest_path(G, source=address, target=dest_addr)
	print("Found shortest path from address %s to address %s with length %d." % (address, dest_addr, len(path)))

	if len(path) > 0:
		return path
	else:
		return None

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

p = Pool()

with open(str(cluster_n) + "_to_" + str(dest_addr) + ".txt", 'w') as f:

	res = set(p.map(find, addresses))
	res.remove(None)

	for new_paths in res:
		paths += new_paths

	# Sort paths by length
	paths.sort(key=len)

	for path in paths:
		f.write("%s\n" % '->'.join(path))
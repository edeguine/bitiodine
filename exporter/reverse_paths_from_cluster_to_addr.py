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
# Cutoff (max depth of a chain to test)
CUTOFF = 100

def find(address):
	paths = list(nx.all_simple_paths(G, source=address, target=dest_addr, cutoff=CUTOFF))
	print("Added %d new paths from address %s to address %s with min length %d." %(len(paths), address, dest_addr, min([len(x) for x in paths])))

	if len(paths) > 0:
		return paths
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

G.reverse(copy=False)
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
#!/usr/bin/env python3
import networkx as nx
import pickle

from sys import argv

# Config
src_addr = argv[1]

with open('../grapher/tx_graph.dat', "rb") as infile:
	G = pickle.load(infile)

G.reverse(copy=False)

print("Graph loaded.")

with open(str(src_addr) + "_to_" + str(dest_addr) + ".txt", 'w') as f:

	paths = list(nx.all_simple_paths(G, source=src_addr))

	print("Added %d new paths from address %s with min length %d." %(len(paths), src_addr, min([len(x) for x in paths])))

	# Sort paths by length
	paths.sort(key=len)

	for path in paths:
		f.write("%s\n" % '->'.join(path))
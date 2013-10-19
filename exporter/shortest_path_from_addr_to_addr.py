#!/usr/bin/env python3
import networkx as nx
import pickle

from sys import argv

# Config
src_addr = argv[1]
dest_addr = argv[2]

with open('../grapher/tx_graph.dat', "rb") as infile:
	G = pickle.load(infile)

print("Graph loaded.")

with open(str(src_addr) + "_to_" + str(dest_addr) + ".txt", 'w') as f:

	path = nx.shortest_path(G, source=src_addr, target=dest_addr)

	print("Found shortest path from address %s to address %s with length %d." % (src_addr, dest_addr, len(path)))

	f.write("%s\n" % '->'.join(path))
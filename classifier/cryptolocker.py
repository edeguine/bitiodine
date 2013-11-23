#!/usr/bin/env python3
from sqlite_wrapper import SQLiteWrapper
from util import *
import pickle
import logging

db = SQLiteWrapper("cryptolocker.db")

known = []
clusters = set()

db_blockchain = SQLiteWrapper("../blockchain/blockchain.sqlite")

with open("cryptolocker_known.txt") as f:
	for addr in f:
		known.append(addr.strip())

print("Known addresses imported.")

with open("../clusterizer/clusters.dat", "rb") as cf:
	users = pickle.load(cf)
print("Clusters loaded.")

for addr in known:
	clusters.add(users[addr])

print("%d clusters found." % len(clusters))

for cluster in clusters:
	print(cluster)

clusters_query = '(' + ', '.join([str(k) for k in clusters]) + ')'

sum_query = "SELECT SUM(txout_value)/1e8 FROM tx_full WHERE address IN " + clusters_query + " AND ((txout_value BETWEEN 1.98e8 AND 2.01e8) OR (txout_value BETWEEN 0.48e8 AND 0.52e8) OR (txout_value BETWEEN 9.98e8 AND 10.02e8));"

detail_query = "SELECT address, COUNT(*) AS ransoms FROM tx_full WHERE address IN " + clusters_query + " AND ((txout_value BETWEEN 1.98e8 AND 2.01e8) OR (txout_value BETWEEN 0.48e8 AND 0.52e8) OR (txout_value BETWEEN 9.98e8 AND 10.02e8)) GROUP BY address ORDER by ransoms DESC;"

sum_res = float(db_blockchain.query(sum_query, fetch_one=True))

print("Sum: %f" % sum_res)
print()
print()

detail_res = db_blockchain.query(detail_query)

for row in detail_res:
	for address, ransoms in row:
		print("%s, %d" % (address, int(ransoms)))

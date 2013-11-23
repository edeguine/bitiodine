#!/usr/bin/env python3
from sqlite_wrapper import SQLiteWrapper
from util import *
import pickle
import logging

db = SQLiteWrapper("cryptolocker.db")

known = set()
clusters = set()

db_blockchain = SQLiteWrapper("../blockchain/blockchain.sqlite")

with open("cryptolocker_known.txt") as f:
	for addr in f:
		known.add(addr.strip())

print("Known addresses imported.")

with open("../clusterizer/clusters.dat", "rb") as cf:
	users = pickle.load(cf)
print("Clusters loaded.")

for addr in known:
	clusters.add(users[addr])

print("%d clusters found." % len(clusters))

for cluster in clusters:
	print(cluster)

# Augment known addresses with addresses in clusters
for address, cluster in users:
	if cluster in clusters:
		known.add(address)

# Free memory
del(users)

clusters_query = '(' + ', '.join([str(k) for k in clusters]) + ')'

sum_query = "SELECT SUM(txout_value)/1e8 FROM tx_full WHERE address IN " + clusters_query + " AND ((txout_value BETWEEN 1.98e8 AND 2.01e8) OR (txout_value BETWEEN 0.48e8 AND 0.52e8) OR (txout_value BETWEEN 9.98e8 AND 10.02e8))"

detail_query = "SELECT address, COUNT(*) AS ransoms FROM tx_full WHERE address IN " + clusters_query + " AND ((txout_value BETWEEN 1.98e8 AND 2.01e8) OR (txout_value BETWEEN 0.48e8 AND 0.52e8) OR (txout_value BETWEEN 9.98e8 AND 10.02e8)) GROUP BY address ORDER by ransoms DESC"

tx_query = "SELECT datetime(time, 'unixepoch'), tx_hash, address FROM tx_full WHERE address IN " + clusters_query + " AND ((txout_value BETWEEN 1.98e8 AND 2.01e8) OR (txout_value BETWEEN 0.48e8 AND 0.52e8) OR (txout_value BETWEEN 9.98e8 AND 10.02e8)) ORDER BY time ASC"

sum_res = float(db_blockchain.query(sum_query, fetch_one=True))

print("Sum: %f" % sum_res)

detail_res = db_blockchain.query(detail_query)
tx_res = db_blockchain.query(tx_query)

with open("cryptolocker_ransoms.txt", "w") as rf:
	for row in detail_res:
		for address, ransoms in row:
			print("%s, %d" % (address, int(ransoms)))
			rf.write("%s, %d" % (address, int(ransoms)))

with open("cryptolocker_tx.txt", "w") as tf:
	for row in detail_res:
		for datetime, tx_hash, address in row:
			print("%s, %s, %s" % (datetime, tx_hash, address))
			tf.write("%s, %s, %s" % (datetime, tx_hash, address))


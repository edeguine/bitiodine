#!/usr/bin/env python3
import networkx as nx

from sqlite_wrapper import SQLiteWrapper
from queries import *
from util import *
from collections import Counter

###

FILENAME = "tx_graph"
db = SQLiteWrapper('../blockchain/blockchain.sqlite')

try:
  max_txid_res = db.query(max_txid_query, fetch_one=True)
except Exception as e:
  die(e)

G = nx.DiGraph()
min_txid = 1

try:
  G, min_txid = load(FILENAME)
except:
  pass

print("Scanning %d transactions, starting from %d." %(max_txid_res, min_txid))

for tx_id in range(min_txid, max_txid_res + 1):

  # Save progress to files
  if tx_id % 500000 == 0:
    print("TRANSACTION ID: %d" % (tx_id))
    save(G, FILENAME, tx_id)
    print("%d nodes, %d edges so far." % (nx.number_of_nodes(G),nx.number_of_edges(G)))

  try:
    in_res = db.query(in_query_addr, (tx_id,))
    out_res = db.query(out_query_addr_with_value, (tx_id,))
  except:
    # Just go to the next transaction
    continue

  # IN
  addresses = []
  for line in in_res:
    address = line[0]
    value = {}
    if address is not None:
    	addresses.append(address)
    else:
      addresses.append("GENERATED")

    # OUT
    # One output transaction case
    try:
      if len(out_res) == 1:
      	value[out_res[0][0]] = float(out_res[0][1]) * 10**-8
    except:
      continue

    # If two outputs, try to predict real recipient
    try:
      if len(out_res) == 2:

        address1 = out_res[0][0]
        address2 = out_res[1][0]

        try:
          appeared1_res = db.query(used_so_far_query, (tx_id, address1), fetch_one=True)
          appeared2_res = db.query(used_so_far_query, (tx_id, address2), fetch_one=True)
          time_res = db.query(time_query, (tx_id,), fetch_one=True)
        except Exception as e:
          die(e)

        if appeared1_res == 0 and (time_res < FIX_TIME or appeared2_res == 1):
      	  value[address1] = float(out_res[0][1]) * 10**-8
        elif appeared2_res == 0 and appeared1_res == 1:
          value[address2] = float(out_res[1][1]) * 10**-8
    except:
      continue

    # If more than two otputs, unable to detect shadow address, add all addresses as recipients
    try:
      if len(out_res) > 2:
        for out in out_res:
          value[out[0]] = float(out[1]) * 10**-8
    except:
      continue

    for r in value:
      G.add_node(r)

    for address in addresses:
      # Update edges
      number_of_transactions = {}
      try:
        for r in value:
          number_of_transactions[r] = G.edge[address][r]['number_of_transactions']
      except:
        pass

      G.add_node(address)

      for r in value:
        G.add_edge(address, r, number_of_transactions=number_of_transactions.get(r, 0)+1)
        G.node[r]['amount_received'] = G.node[r].get('amount_received', 0) + value[r]

save(G, FILENAME, tx_id)
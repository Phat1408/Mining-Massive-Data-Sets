#!/usr/bin/python3

import sys

# currency looks like 1,000,000.00
to_numeric = lambda x: float(x.replace(",",""))

key_region = "South-East Asia"
for line in sys.stdin:
	tokens = line.strip().split('\t')
	region = tokens[1]
	if region == key_region:
		cum = to_numeric(tokens[7])
		print(f"%s\t%f" % (key_region, cum))
	else: continue

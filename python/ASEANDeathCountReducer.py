#!/usr/bin/python3

import sys

key_region = "South-East Asia"
result = 0.00

for line in sys.stdin:
	try:
		_, val = line.strip().split("\t")
		result += float(val)
	except: continue

print("%s\t%f" % (key_region, result))

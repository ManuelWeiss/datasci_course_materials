from MapReduce import *
import sys

mr = MapReduce()

def mapper(record):
	(matrix, i, j, value) = record
	if matrix == "a":
		for k in xrange(100):
		    mr.emit_intermediate((i, k), ("a", j, value))
	else:
		for k in xrange(100):
		    mr.emit_intermediate((k, j), ("b", i, value))

def reducer(key, list_of_values):
	a = {}
	b = {}
	for i in list_of_values:
		if i[0] == "a":
			a[i[1]] = i[2]
		else:
			b[i[1]] = i[2]
	total = 0
	for i in a.keys():
		total += a[i] * b.get(i, 0)
	if total > 0:
		mr.emit((key[0], key[1], total))

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
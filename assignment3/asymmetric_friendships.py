from MapReduce import *
import sys

mr = MapReduce()

def mapper(record):
    mr.emit_intermediate(record[0], record[1])
    mr.emit_intermediate(record[1], record[0])

def reducer(key, list_of_values):
	for p in set(list_of_values):
		mr.emit((key, p))

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
from MapReduce import *
import sys

mr = MapReduce()

def mapper(record):
    mr.emit_intermediate((record[0], record[1]), 1)
    mr.emit_intermediate((record[1], record[0]), 1)

def reducer(key, list_of_values):
	if len(list_of_values) > 1:
		return
	mr.emit(key)

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
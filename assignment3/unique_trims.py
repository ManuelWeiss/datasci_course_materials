from MapReduce import *
import sys

mr = MapReduce()

def mapper(record):
    mr.emit_intermediate(record[1][:-10], 1)

def reducer(key, list_of_values):
		mr.emit(key)

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
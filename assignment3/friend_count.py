from MapReduce import *
import sys

mr = MapReduce()

def mapper(record):
    mr.emit_intermediate(record[0], record)

def reducer(key, list_of_values):
	mr.emit((key, len(list_of_values)))

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
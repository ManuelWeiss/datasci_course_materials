from MapReduce import *
import sys

mr = MapReduce()

def mapper(record):
    order_id = record[1]
    mr.emit_intermediate(order_id, record)

def reducer(order_id, list_of_values):
	order = list()
	line_items = list()
	for i in list_of_values:
		if i[0] == "order":
			order = i
		else:
			line_items.append(i)
	for i in line_items:
		mr.emit(order + i)

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
from MapReduce import *
import sys

mr = MapReduce()

def mapper(record):
    docid = record[0]
    text = record[1]
    words = text.split()
    for w in words:
      mr.emit_intermediate(w, docid)

def reducer(word, list_of_values):
    doc_list = set()
    for v in list_of_values:
      doc_list.add(v)
    mr.emit((word, list(doc_list)))

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
#!/usr/bin/env python
#
# Not used but leaving around since it has an MR example
#

import os
import sys
from configobj import ConfigObj
#
import errno
from pymongo.mongo_client import MongoClient
from _collections import defaultdict
import json
import pymongo
from bson.code import Code
from bson.objectid import ObjectId

# where to get credentials (don't check these into git, idiot)
CFG_FILE_DEFAULT = 'shock_usage.cfg'
CFG_SECTION_SOURCE = 'SourceMongo'

CFG_HOST = 'host'
CFG_PORT = 'port'
CFG_DB = 'db'
CFG_USER = 'user'
CFG_PWD = 'pwd'

config = ConfigObj(CFG_FILE_DEFAULT)

srcconf=config[CFG_SECTION_SOURCE]
host=srcconf[CFG_HOST]
port=int(srcconf[CFG_PORT])
db=srcconf[CFG_DB]
user=srcconf[CFG_USER]
pwd=srcconf[CFG_PWD]

srcmongo = MongoClient(host,port, slaveOk=True)
srcdb=srcmongo[db]
srcdb.authenticate(user,pwd)


map = Code("function() { \
                       d=this._id.getTimestamp(); \
                       key=d.getFullYear()*100+(d.getMonth()+1); \
                       emit(key, this.file.size); \
                   };") 

reduce = Code("function(key, size) { return Array.sum(size); };")

results = srcdb.Nodes.inline_map_reduce(map, reduce) # , query={'_id' : ObjectId("5032b13bae207ff717ac28a2")})

out={}
cum=0
out['by_month']={}
for result in sorted(results):
    month=int(result['_id'])
    out['by_month'][month]={}
    out['by_month'][month]['bytes_new']=result['value']
    out['by_month'][month]['bytes_cumulative']+=result['value']

print json.dumps(out,indent=2,sort_keys=True)
#for doc in result.find():
#	print doc

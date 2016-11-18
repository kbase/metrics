#!/usr/bin/env python
# encoding: utf-8
'''
Lists all users in KBase by querying all known databases that store user names.
Requires access to a KBase deployment.cfg file to find and authenticate to the
databases.

Current data sources are:
Workspace 0.5.0
UJS 0.2.1
Shock
AWE
Handle Service (ignored. The chance a user is in HS and not in WS or Shock is virtually zero)
User Profile Service
Catalog
Narrative Method Store

Takes one argument: the path to the KBase deployment.cfg / cluster.ini file
that contains the configurations for the KBase services above. The code
uses those configurations to talk directly to the mongo database for each
service.
'''
from ConfigParser import ConfigParser
import sys
from pymongo.mongo_client import MongoClient


def proc_workspace(cfg):
    ws = 'Workspace'
    mongohost = cfg.get(ws, 'mongodb-host')
    mongodb = cfg.get(ws, 'mongodb-database')
    u = 'mongodb-user'
    mongouser = None
    if cfg.has_option(ws, u):
        mongouser = cfg.get(ws, u)
    p = 'mongodb-pwd'
    mongopwd = None
    if cfg.has_option(ws, p):
        mongopwd = cfg.get(ws, p)
    mcli = MongoClient(mongohost, slaveOk=True)
    db = mcli[mongodb]
    if mongouser:
        db.authenticate(mongouser, mongopwd)

    ret = db.workspaceACLs.aggregate([{'$group': {'_id': None, 'users': {'$push': '$user'}}}])
    users = set(ret['result'][0]['users'])
    print 'workspaceACLs'
    print users
    uprov = set()
    count = 0
    for p in db.provenance.find(fields=['user']):  # aggregation runs out of memory
        uprov.add(p['user'])
        count += 1
        if count % 100000 == 0:
            print 'At provenance record ' + str(count)
    print 'provenance'
    print uprov
    users.union(uprov)
    uobjs = set()
    count = 0
    for p in db.workspaceObjVersions.find(fields=['savedby']):  # aggregation runs out of memory
        uobjs.add(p['savedby'])
        count += 1
        if count % 100000 == 0:
            print 'At object version record ' + str(count)
    print 'object versions'
    print uobjs
    users.union(uobjs)
    return users


def proc_ujs(cfg):
    return set()


def main():
    cfg = ConfigParser()
    cfg.read(sys.argv[1])
    names = proc_workspace(cfg)
    names.union(proc_ujs(cfg))
    print names

if __name__ == "__main__":
    main()

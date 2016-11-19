#!/usr/bin/env python
# encoding: utf-8
'''
Lists all users in KBase by querying all known databases that store user names.
Requires access to a KBase deployment.cfg file to find and authenticate to the
databases.

Current data sources are:
Workspace 0.5.0
UJS 0.2.1
Shock 0.9.6
AWE 0.9.11
Handle Service (ignored. The chance a user is in HS and not in WS or Shock is virtually zero)
User Profile Service a74c3ced0abdec6a986dbde1a04a95ae4c791a90
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


def get_mongo_db(cfg, section, hostkey, dbkey, userkey, pwdkey):
    mongohost = cfg.get(section, hostkey)
    mongodb = cfg.get(section, dbkey)
    mongouser = None
    if cfg.has_option(section, userkey):
        mongouser = cfg.get(section, userkey)
    mongopwd = None
    if cfg.has_option(section, pwdkey):
        mongopwd = cfg.get(section, pwdkey)
    mcli = MongoClient(mongohost, slaveOk=True)
    db = mcli[mongodb]
    if mongouser:
        db.authenticate(mongouser, mongopwd)
    return db


def proc_workspace(cfg):
    db = get_mongo_db(cfg, 'Workspace', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.workspaceACLs.aggregate([{'$group': {'_id': None, 'users': {'$push': '$user'}}}])
    users = set(ret['result'][0]['users'])
    uprov = set()
    count = 0
    for p in db.provenance.find(fields=['user']):  # aggregation runs out of memory
        uprov.add(p['user'])
        count += 1
        if count % 100000 == 0:
            print 'At provenance record ' + str(count)
    users.update(uprov)
    uobjs = set()
    count = 0
    for p in db.workspaceObjVersions.find(fields=['savedby']):  # aggregation runs out of memory
        uobjs.add(p['savedby'])
        count += 1
        if count % 100000 == 0:
            print 'At object version record ' + str(count)
    users.update(uobjs)
    return users


def proc_ujs(cfg):
    db = get_mongo_db(cfg, 'UserAndJobState', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.userstate.aggregate([{'$group': {'_id': None, 'users': {'$push': '$user'}}}])
    users = set(ret['result'][0]['users'])
    ujobs = set()
    count = 0
    for j in db.jobstate.find():
        ujobs.add(j['user'])
        cb = j.get('canceledby')
        if cb:
            ujobs.add(cb)
        sh = j.get('shared')
        if sh:
            ujobs.update(sh)
        count += 1
        if count % 100000 == 0:
            print 'At job record ' + str(count)
    users.update(ujobs)
    return set()


def proc_shock(cfg):
    db = get_mongo_db(cfg, 'shock', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.Users.aggregate([{'$group': {'_id': None, 'users': {'$push': '$username'}}}])
    users = set(ret['result'][0]['users'])
    return users


def proc_awe(cfg):
    db = get_mongo_db(cfg, 'awe', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.Users.aggregate([{'$group': {'_id': None, 'users': {'$push': '$username'}}}])
    users = set(ret['result'][0]['users'])
    print 'awe'
    print users
    return users


def proc_userprof(cfg):
    db = get_mongo_db(cfg, 'UserProfile', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.profiles.aggregate([{'$group': {'_id': None, 'users': {'$push': '$user.username'}}}])
    users = set(ret['result'][0]['users'])
    print 'userprofile'
    print users
    return users


def main():
    cfg = ConfigParser()
    cfg.read(sys.argv[1])
    names = proc_workspace(cfg)
    names.update(proc_ujs(cfg))
    names.update(proc_shock(cfg))
#     names.update(proc_awe(cfg))  # TODO awe deploy entry is broken right now, need help from kk
    names.update(proc_userprof(cfg))
    names.remove('*')
    print names

if __name__ == "__main__":
    main()

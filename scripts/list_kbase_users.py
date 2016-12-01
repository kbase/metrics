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
Catalog 2.0.5
Narrative Method Store 0.3.5 (note Catalog and NMS share the catalog DB but have separate
    collections)

Takes one argument: the path to the KBase deployment.cfg / cluster.ini file
that contains the configurations for the KBase services above. The code
uses those configurations to talk directly to the mongo database for each
service.
'''
from ConfigParser import ConfigParser
import sys
from pymongo.mongo_client import MongoClient
from _collections import defaultdict


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


def make_set_from_agg_result(incoming, field_name):
    if not incoming['result']:
        return set()
    return set(incoming['result'][0][field_name])


def proc_workspace(cfg):
    db = get_mongo_db(cfg, 'Workspace', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.workspaceACLs.aggregate([{'$group': {'_id': None, 'users': {'$push': '$user'}}}])
    users = make_set_from_agg_result(ret, 'users')

    ret = db.admins.aggregate([{'$group': {'_id': None, 'users': {'$push': '$user'}}}])
    admins = make_set_from_agg_result(ret, 'users')
    users.update(admins)

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
    users.remove('*')
    return users


def proc_ujs(cfg):
    db = get_mongo_db(cfg, 'UserAndJobState', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.userstate.aggregate([{'$group': {'_id': None, 'users': {'$push': '$user'}}}])
    users = make_set_from_agg_result(ret, 'users')

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
    return users


def proc_shock(cfg):
    db = get_mongo_db(cfg, 'shock', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.Users.aggregate([{'$group': {'_id': None, 'users': {'$push': '$username'}}}])
    users = make_set_from_agg_result(ret, 'users')
    users.remove('*')
    users.remove('')
    users.remove('1')
    return users


def proc_awe(cfg):
    db = get_mongo_db(cfg, 'awe', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.Users.aggregate([{'$group': {'_id': None, 'users': {'$push': '$username'}}}])
    users = make_set_from_agg_result(ret, 'users')
    return users


def proc_userprof(cfg):
    db = get_mongo_db(cfg, 'UserProfile', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.profiles.aggregate([{'$group': {'_id': None, 'users': {'$push': '$user.username'}}}])
    users = make_set_from_agg_result(ret, 'users')
    return users


def proc_catalog_nms(cfg):
    db = get_mongo_db(cfg, 'catalog', 'mongodb-host', 'mongodb-database', 'mongodb-user',
                      'mongodb-pwd')

    ret = db.developers.aggregate([{'$group': {'_id': None, 'users': {'$push': '$kb_username'}}}])
    users = make_set_from_agg_result(ret, 'users')

    ret = db.exec_stats_raw.aggregate([{'$group': {'_id': None, 'users': {'$push': '$user_id'}}}])
    ustatsraw = make_set_from_agg_result(ret, 'users')
    users.update(ustatsraw)

    ret = db.exec_stats_users.aggregate([{'$group': {'_id': None, 'users':
                                                     {'$push': '$user_id'}}}])
    ustatsuser = make_set_from_agg_result(ret, 'users')
    users.update(ustatsuser)

    ret = db.favorites.aggregate([{'$group': {'_id': None, 'users': {'$push': '$user'}}}])
    ufav = make_set_from_agg_result(ret, 'users')
    users.update(ufav)

    authors = set()
    count = 0
    for a in db.local_functions.find():
        authors.update(a['authors'])
        count += 1
        if count % 100000 == 0:
            print 'At local_functions record ' + str(count)
    users.update(authors)

    modowners = set()
    count = 0
    for a in db.modules.find():
        for o in a['owners']:
            modowners.add(o['kb_username'])
        count += 1
        if count % 100000 == 0:
            print 'At modules record ' + str(count)
    users.update(modowners)

    repoowners = set()
    count = 0
    for r in db.repo_history.find():
        repoowners.update(r['repo_data']['owners'])
        count += 1
        if count % 100000 == 0:
            print 'At repo_history record ' + str(count)
    users.update(repoowners)

    return users


def update_names(store, names, service):
    for n in names:
        store[n].add(service)


def main():
    cfg = ConfigParser()
    cfg.read(sys.argv[1])
    d = defaultdict(set)
    update_names(d, proc_workspace(cfg), 'Workspace')
    update_names(d, proc_ujs(cfg), 'UJS')
    update_names(d, proc_shock(cfg), 'Shock')
    update_names(d, proc_awe(cfg), 'AWE')
    update_names(d, proc_userprof(cfg), 'User Profile')
    update_names(d, proc_catalog_nms(cfg), 'Catalog/NMS')
    for n in sorted(d.keys()):
        line = [n]
        line.extend(sorted(d[n]))
        print ', '.join(line)

if __name__ == "__main__":
    main()

#!/usr/bin/env python

'''
Created on Oct 14, 2014

@author: gaprice@lbl.gov

Calculate shock disk usage and object counts  by user, separated into
public vs. private data.

These figures may not be actually related to the physical disk space since
shock has the ability to copy a node, which I believe makes the equivalent
of a hard link.

Don't run this during high loads - runs through every object in the DB
Hasn't been optimized much either.
'''

# TODO share code with the workspace aggregator
# TODO: checks to see this is accurate
# TODO: some basic sanity checking

from __future__ import print_function
from argparse import ArgumentParser
import os
import sys
from bzrlib.config import ConfigObj
import errno
from pymongo.mongo_client import MongoClient
import time
from _collections import defaultdict
import json
from datetime import date


# where to get credentials (don't check these into git, idiot)
CFG_FILE_DEFAULT = 'shock_usage.cfg'
CFG_SECTION_SOURCE = 'SourceMongo'
CFG_SECTION_TARGET = 'TargetMongo'

CFG_HOST = 'host'
CFG_PORT = 'port'
CFG_DB = 'db'
CFG_USER = 'user'
CFG_PWD = 'pwd'

CFG_EXCLUDE_USER = 'exclude-user'
CFG_STAFF_FILE = 'staff-file'

# output file names
USER_FILE = 'shock_data.json'

# collection names
COL_USER = 'Users'
COL_NODE = 'Nodes'

# field names
USER_UUID = 'uuid'
USER_NAME = 'username'
NODE_OWNER = 'acl.owner'
NODE_READ = 'acl.read'
NODE_SIZE = 'file.size'

PUBLIC = 'pub'
PRIVATE = 'priv'
STAFF = ':staff'
USER = ':user'
OBJ_CNT = 'cnt'
BYTES = 'byte'

NO_OWNER = '__NONE__'

MAX_NODES_PER_CALL = 10000

staff = {}

def _parseArgs():
    parser = ArgumentParser(description='Calculate shock disk usage by ' +
                                        'user')
    parser.add_argument('-c', '--config',
                        help='path to the config file. By default the ' +
                        'script looks for a file called ' + CFG_FILE_DEFAULT +
                        ' in the working directory.',
                        default=CFG_FILE_DEFAULT)
    parser.add_argument('-o', '--output',
                        help='write json output to this directory. If it ' +
                        'does not exist it will be created.')
    return parser.parse_args()


# http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def process_optional_key(configObj, section, key):
    v = configObj[section].get(key)
    v = None if v == '' else v
    configObj[section][key] = v
    return v


def get_config(cfgfile):
    if not os.path.isfile(cfgfile) and not os.access(cfgfile, os.R_OK):
        print('Cannot read file ' + cfgfile)
        sys.exit(1)
    co = ConfigObj(cfgfile)
    s = CFG_SECTION_SOURCE
    t = CFG_SECTION_TARGET

    for sec in (s, t):
        if sec not in co:
            print('Missing config section {} from file {}'.format(
                  sec, cfgfile))
            sys.exit(1)
        for key in (CFG_HOST, CFG_PORT, CFG_DB):
            v = co[sec].get(key)
            if v == '' or v is None:
                print('Missing config value {}.{} from file {}'.format(
                    sec, key, cfgfile))
                sys.exit(1)
        try:
            co[sec][CFG_PORT] = int(co[sec][CFG_PORT])
        except ValueError:
            print('Port {} is not a valid port number at {}.{}'.format(
                co[sec][CFG_PORT], sec, CFG_PORT))
            sys.exit(1)
    for sec in (s, t):
        u = process_optional_key(co, sec, CFG_USER)
        p = process_optional_key(co, sec, CFG_PWD)
        if u is not None and p is None:
            print ('If {} specified, {} must be specified in section '.format(
                CFG_USER, CFG_PWD) + '{} from file {}'.format(sec, cfgfile))
            sys.exit(1)

    excl_users = co[s][CFG_EXCLUDE_USER]
    if excl_users:
        if type(excl_users) is not list:
            co[s][CFG_EXCLUDE_USER] = set([excl_users])
        else:
            co[s][CFG_EXCLUDE_USER] = set(excl_users)
    return co[s], co[t]


def make_and_check_output_dir(outdir):
    if outdir:
        try:
            mkdir_p(outdir)
        except Exception as e:
            print(e.__repr__())
            print("Couldn't create or read output directory {}: {}".format(
                outdir, e.strerror))
            sys.exit(1)
        if not os.path.isdir(outdir) or not os.access(outdir, os.W_OK):
            print('Cannot write to directory ' + outdir)
            sys.exit(1)


def processNames(srcdb, excluded_names):
    excluded = []
    uuid2name = {}
    # may need to batch this a long time from now
    for u in srcdb[COL_USER].find({}, [USER_UUID, USER_NAME, 'name']):
        if not u.get(USER_NAME):
            continue
        uuid2name[u[USER_UUID]] = u[USER_NAME]
        if u[USER_NAME] in excluded_names:
            excluded.append(u[USER_UUID])
    return uuid2name, excluded


def processNodeRecs(userdata, recs, uuid2name, excludedUUIDs):
    acl = 'acl'
    read = 'read'
    owner = 'owner'
    file_ = 'file'
    size = 'size'
    count = 0
    ttl = 0
    t = time.time()
    for rec in recs:
        if ttl % 10000 == 0:
            print("Processed {} records, kept {} in {} s".format(
                ttl, count, time.time() - t))
            sys.stdout.flush()
        ttl += 1
        s = rec[file_][size]
        o = rec[acl].get(owner)
        o_str = str(rec['_id']) 
        id_time = int(o_str[0:8], 16)
        month=date.fromtimestamp(id_time).strftime('%Y%m')

        if o in excludedUUIDs:
            continue
        if not o:
            o = NO_OWNER
        else:
            o = uuid2name[o]
        r = rec[acl][read]
        pub = PUBLIC if len(r) == 0 else PRIVATE
        userdata['by_user'][o][pub][OBJ_CNT] += 1
        userdata['by_user'][o][pub][BYTES] += s
        userdata['by_month'][month][pub][OBJ_CNT] += 1
        userdata['by_month'][month][pub][BYTES] += s
        if o in staff:
            pub=pub+STAFF
        else:
            pub=pub+USER
        
        userdata['by_month'][month][pub][OBJ_CNT] += 1
        userdata['by_month'][month][pub][BYTES] += s
        count += 1


def processNodes(srcdb, uuid2name, excludedUUIDs):
    d = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int))))

    # turns out the stupid query is the fastest, trying to page via UUID
    # prefixes is way slower (confirmed was only scanning ~2k records via
    # explain(), so not sure why it's so slow - was taking 50s for the 2k
    # records).
    # See previous commits for those implementations.
    # Note skip() / limit() don't scale:
    # http://docs.mongodb.org/manual/reference/method/cursor.skip/
    # this approach won't work for most cases - only useful if you want
    # to scan the whole collection and can let mongo do the batching for you.\

    recs = srcdb[COL_NODE].find({NODE_OWNER: {'$nin': excludedUUIDs}},
                                [NODE_OWNER, NODE_READ, NODE_SIZE])
    processNodeRecs(d, recs, uuid2name, excludedUUIDs)
    cum=defaultdict(lambda: defaultdict(int))
    for month in sorted(d['by_month']):
        types=d['by_month'][month].keys();
        for type in (PUBLIC,PRIVATE,PUBLIC+STAFF,PRIVATE+STAFF,PUBLIC+USER,PRIVATE+USER) :
            for acc in ("byte","cnt"):
               cum[type][acc]+=d['by_month'][month][type][acc]
               d['by_month'][month]['cumulative_'+type][acc]=cum[type][acc]
               
        
    return d

def processStaff(file):
    f=open(file)
    for name in f:
        staff[name.rstrip('\n')]=True 

def main():
    args = _parseArgs()
    outdir = args.output
    make_and_check_output_dir(outdir)
    sourcecfg, targetcfg = get_config(args.config)  # @UnusedVariable
    starttime = time.time()
    srcmongo = MongoClient(sourcecfg[CFG_HOST], sourcecfg[CFG_PORT],
                           slaveOk=True)
    srcdb = srcmongo[sourcecfg[CFG_DB]]
    if sourcecfg[CFG_USER]:
        srcdb.authenticate(sourcecfg[CFG_USER], sourcecfg[CFG_PWD])
    print('Processing user names... ', end='')
    uuid2name, excludedUUIDs = processNames(srcdb, sourcecfg[CFG_EXCLUDE_USER])
    print('done.')
    if CFG_STAFF_FILE in sourcecfg:
      print('Processing staff file ',sourcecfg[CFG_STAFF_FILE])
      processStaff(sourcecfg[CFG_STAFF_FILE])

    userdata = processNodes(srcdb, uuid2name, excludedUUIDs)
    userdata['meta']['comments']='This data comes from shock and filters out the workspace objects'
    userdata['meta']['author']='Gavin Price, Jared Bischof, Shane Canon'
    userdata['meta']['description']='Summary of amount of data stored in shock both by user and by month'

    if outdir:
        with open(os.path.join(outdir, USER_FILE), 'w') as f:
            f.write(json.dumps(userdata,indent=2,sort_keys=True))

    print('\nElapsed time: ' + str(time.time() - starttime))


if __name__ == '__main__':
    main()

'''
user
    pub
        cnt
        byte
    priv
        cnt
        byte
__none__
    pub
        cnt
        byte
'''

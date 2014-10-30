#!/usr/bin/env python

'''
Created on Apr 27, 2014

@author: gaprice@lbl.gov

Calculate workspace disk usage and object counts  by user, separated into
public vs. private and deleted vs. undeleted data.

These figures are not actually related to the physical disk space for three
reasons:
1) The workspace saves space by only keeping one copy of each unique
    document. From the perspective of user disk usage, this feature is ignored.
2) Copies in the workspace are copies by reference, not by value. Again,
    from the perspective of user disk usage, this feature is ignored.
3) Only actual data objects are included (e.g. data stored in GridFS or Shock).
    Any data stored in MongoDB (other than GridFS files) is not included.

All versions are included in the counts and disk usage statistics.

Don't run this during high loads - runs through every object in the DB
Hasn't been optimized much either, could probably optimize by not querying
ws by ws.
'''

# TODO: checks to see this is accurate
# TODO: some basic sanity checking

from __future__ import print_function
from configobj import ConfigObj
from pymongo import MongoClient
import time
import sys
import os
from collections import defaultdict
import datetime
from argparse import ArgumentParser
import json
import errno

# where to get credentials (don't check these into git, idiot)
CFG_FILE_DEFAULT = 'usage.cfg'
CFG_SECTION_SOURCE = 'SourceMongo'
CFG_SECTION_TARGET = 'TargetMongo'

CFG_HOST = 'host'
CFG_PORT = 'port'
CFG_DB = 'db'
CFG_USER = 'user'
CFG_PWD = 'pwd'

CFG_TYPES = 'types'
CFG_LIST_OBJS = 'list-objects'
CFG_EXCLUDE_WS = 'exclude-ws'

# output file names
USER_FILE = 'user_data.json'
WS_FILE = 'ws_data.json'
OBJECT_FILE = 'ws_object_list.json'

# collection names
COL_WS = 'workspaces'
COL_ACLS = 'workspaceACLs'
COL_OBJ = 'workspaceObjects'
COL_VERS = 'workspaceObjVersions'

# workspace fields
WS_OBJ_CNT = 'numObj'
WS_DELETED = 'del'
WS_OWNER = 'owner'
WS_ID = 'ws'
WS_NAME = 'name'

# program fields
PUBLIC = 'pub'
PRIVATE = 'priv'
OBJ_CNT = 'cnt'
BYTES = 'byte'
DELETED = WS_DELETED
NOT_DEL = 'std'
OWNER = WS_OWNER
TYPES = 'types'
NAME = WS_NAME
SHARED = 'shd'


LIMIT = 10000
OR_QUERY_SIZE = 100  # 75 was slower, 150 was slower
MAX_WS = 10  # for testing, set to < 1 for all ws


def _parseArgs():
    parser = ArgumentParser(description='Calculate workspace disk usage by ' +
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


def chunkiter(iterable, size):
    """Iterates over an iterable in chunks of size size. Returns an iterator
  that in turn returns iterators over the iterable that each iterate through
  size objects in the iterable.
  Note that since the inner and outer loops are pulling values from the same
  iterator, continue and break don't necessarily behave exactly as one would
  expect. In the outer loop of the iteration, continue effectively does
  nothing, but break works normally. In the inner loop, break has no real
  effect but continue works normally. For the latter issue, wrapping the inner
  iterator in a tuple will cause break to skip the remaining items in the
  iterator. Alternatively, one can set a flag and exhaust the inner iterator.
  """
    def inneriter(first, iterator, size):
        yield first
        for _ in xrange(size - 1):
            yield iterator.next()
    it = iter(iterable)
    while True:
        yield inneriter(it.next(), it, size)


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

    process_config_string_list(CFG_TYPES, co[s])
    process_config_string_list(CFG_LIST_OBJS, co[s])

    exclude = co[s][CFG_EXCLUDE_WS]
    if exclude:
        if type(exclude) is not list:
            exclude = [exclude]
        ints = set()
        for ws in exclude:
            try:
                ints.add(int(ws))
            except ValueError:
                print ('Workspace id {} must be an integer'.format(ws))
                sys.exit(1)
        co[s][CFG_EXCLUDE_WS] = ints
    return co[s], co[t]


def process_config_string_list(config_name, config_section):
    c = config_section[config_name]
    if c:
        if type(c) is not list:
            config_section[config_name] = set([c])
        else:
            config_section[config_name] = set(c)
    else:
        config_section[config_name] = None


# this might need to be batched at some point
def process_workspaces(db):
    user = 'user'
    all_users = '*'
    acl_id = 'id'
    ws_cursor = db[COL_WS].find({}, [WS_ID, WS_OBJ_CNT, WS_OWNER, WS_DELETED,
                                     NAME])
    pub_read = db[COL_ACLS].find({user: all_users}, [acl_id])
    workspaces = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for ws in ws_cursor:
        # this could be faster via batching
        workspaces[ws[WS_ID]][SHARED] = \
            db[COL_ACLS].find({acl_id: ws[WS_ID]}).count() - 1
        workspaces[ws[WS_ID]][PUBLIC] = PRIVATE
        workspaces[ws[WS_ID]][WS_OBJ_CNT] = ws[WS_OBJ_CNT]
        workspaces[ws[WS_ID]][OWNER] = ws[WS_OWNER]
        workspaces[ws[WS_ID]][NAME] = ws[NAME]
    for pr in pub_read:
        workspaces[pr[acl_id]][PUBLIC] = PUBLIC
        if workspaces[pr[acl_id]][SHARED] > 0:
            workspaces[pr[acl_id]][SHARED] -= 1
    return workspaces


# this method sig is getting way to big
def process_object_versions(db, userdata, typedata, objects, workspaces,
                            incl_types, start_id, end_id):
    # note all objects are from the same workspace
    obj_id = 'id'
    size = 'size'
    type_ = 'type'

    odel = {}
    for o in objects:
        odel[o[obj_id]] = o[DELETED]
    if not odel:
        return 0

    ws = o[WS_ID]  # all objects in same ws
    wsowner = workspaces[ws][OWNER]
    wspub = workspaces[ws][PUBLIC]

    res = db[COL_VERS].find({WS_ID: ws,
                             obj_id: {'$gt': start_id, '$lte': end_id}},
                            [WS_ID, obj_id, size, type_])
    vers = 0
    for v in res:
        if v[obj_id] not in odel:  # new object was made just now in ws
            continue
        vers += 1
        deleted = DELETED if odel[v[obj_id]] else NOT_DEL
        userdata[wsowner][wspub][deleted][OBJ_CNT] += 1
        userdata[wsowner][wspub][deleted][BYTES] += v[size]
        workspaces[ws][deleted][OBJ_CNT] += 1
        workspaces[ws][deleted][BYTES] += v[size]
        t = v[type_].split('-')[0]
        if t in incl_types:
            typedata[wsowner][t][wspub][deleted][OBJ_CNT] += 1
            typedata[wsowner][t][wspub][deleted][BYTES] += v[size]
    return vers


def process_objects(db, workspaces, exclude_ws, incl_types):
    ws_id = 'ws'
    obj_id = 'id'
    # user -> pub -> del -> du or objs -> #
    d = defaultdict(lambda: defaultdict(lambda: defaultdict(
        lambda: defaultdict(int))))
    # user -> type -> pub -> del -> du or objs -> #
    types = defaultdict(lambda: defaultdict(lambda: defaultdict(
        lambda: defaultdict(lambda: defaultdict(int)))))
    wscount = 0
    for ws in workspaces:
        if MAX_WS > 0 and wscount > MAX_WS:
            break
        wscount += 1
        wsobjcount = workspaces[ws][WS_OBJ_CNT]
        print('\nProcessing workspace {}, {} objects'.format(
            ws, wsobjcount))
        if ws in exclude_ws:
            print('\tIn exclude list, skipping')
            continue
        for lim in xrange(LIMIT, wsobjcount + LIMIT, LIMIT):
            print('\tProcessing objects {} - {} at {}'.format(
                lim - LIMIT + 1, wsobjcount if lim > wsobjcount else lim,
                datetime.datetime.now()))
            sys.stdout.flush()
            objtime = time.time()
            query = {ws_id: ws, obj_id: {'$gt': lim - LIMIT, '$lte': lim}}
            objs = db[COL_OBJ].find(query, [ws_id, obj_id, WS_DELETED])
            print('\ttotal obj query time: ' + str(time.time() - objtime))
            ttlstart = time.time()
            vers = process_object_versions(db, d, types, objs, workspaces,
                                           incl_types, lim - LIMIT, lim)

            print('\ttotal ver query time: ' + str(time.time() - ttlstart))
            print('\ttotal object versions: ' + str(vers))
#             print('\tobjects processed: ' + str(objsproc))
#             objcount += objsproc
#             print('total objects processed: ' + str(objcount))
            sys.stdout.flush()
    return d, types


# from https://gist.github.com/lonetwin/4721748
def print_table(rows):
    """print_table(rows)

    Prints out a table using the data in `rows`, which is assumed to be a
    sequence of sequences with the 0th element being the header.
    """

    # - figure out column widths
    widths = [len(max(columns, key=len)) for columns in zip(*rows)]

    # - print the header
    header, data = rows[0], rows[1:]
    print(' | '.join(format(title, "%ds" % width) for width, title in zip(widths, header))) #@IgnorePep8

    # - print the separator
    print('-+-'.join('-' * width for width in widths))

    # - print the data
    for row in data:
        print(" | ".join(format(cdata, "%ds" % width) for width, cdata in zip(widths, row))) #@IgnorePep8


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


def main():
    args = _parseArgs()
    outdir = args.output
    make_and_check_output_dir(outdir)
    sourcecfg, targetcfg = get_config(args.config)  # @UnusedVariable
    print(sourcecfg)
    starttime = time.time()
    srcmongo = MongoClient(sourcecfg[CFG_HOST], sourcecfg[CFG_PORT],
                           slaveOk=True)
    srcdb = srcmongo[sourcecfg[CFG_DB]]
    if sourcecfg[CFG_USER]:
        srcdb.authenticate(sourcecfg[CFG_USER], sourcecfg[CFG_PWD])
    ws = process_workspaces(srcdb)

    objdata, typedata = process_objects(srcdb, ws, sourcecfg[CFG_EXCLUDE_WS],
                                        sourcecfg[CFG_TYPES])

    for wsid in ws:
        del ws[wsid][WS_OBJ_CNT]
    for u in objdata:
        objdata[u][TYPES] = typedata[u]
    if outdir:
        with open(os.path.join(outdir, USER_FILE), 'w') as f:
            f.write(json.dumps(objdata))
        with open(os.path.join(outdir, WS_FILE), 'w') as f:
            f.write(json.dumps(ws))

#     rows = [['user',
#              'pub-bytes', 'pub-#', 'pub-del-bytes', 'pub-del-#',
#              'priv-bytes', 'priv-#', 'priv-del-bytes', 'priv-del-#']]

    print('\nElapsed time: ' + str(time.time() - starttime))
#     for name_ in sorted(objdata):
#         row = [name_]
#         rows.append(row)
#         row.append(str(objdata[name_][PUBLIC][NOT_DEL][BYTES]))
#         row.append(str(objdata[name_][PUBLIC][NOT_DEL][OBJ_CNT]))
#         row.append(str(objdata[name_][PUBLIC][DELETED][BYTES]))
#         row.append(str(objdata[name_][PUBLIC][DELETED][OBJ_CNT]))
#         row.append(str(objdata[name_][PRIVATE][NOT_DEL][BYTES]))
#         row.append(str(objdata[name_][PRIVATE][NOT_DEL][OBJ_CNT]))
#         row.append(str(objdata[name_][PRIVATE][DELETED][BYTES]))
#         row.append(str(objdata[name_][PRIVATE][DELETED][OBJ_CNT]))

#     print_table(rows)
    # print time, object data

if __name__ == '__main__':
    main()

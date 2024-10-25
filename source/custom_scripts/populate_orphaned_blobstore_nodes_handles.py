from pymongo import MongoClient
from pymongo import ReadPreference
#from biokbase.workspace.client import Workspace
#from installed_clients.AbstractHandleClient import AbstractHandle as HandleService
from biokbase.service.Client import Client as ServiceClient
import json as _json
import os
import mysql.connector as mysql
import requests
import time
#from splitting import split_sequence
from datetime import date
from datetime import datetime

print("############################################")
print("############################################")
print("############################################")
print("START TIME (UTC): " + str(datetime.utcnow()))
start_time = time.time()

requests.packages.urllib3.disable_warnings()

mongoDB_metrics_connection = os.environ["MONGO_PATH"]

#ws_url = os.environ["WS_URL"]
ws_user_token = os.environ["METRICS_WS_USER_TOKEN"]
to_workspace = os.environ["WRK_SUFFIX"]

to_blobstore = os.environ["BLOBSTORE_SUFFIX"]
to_handle_db = os.environ["HANDLE_DB_SUFFIX"]


client = MongoClient(mongoDB_metrics_connection + to_workspace)
db = client.workspace
handle_service_url = "https://kbase.us/services/handle_service"

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]
# connect to mysql
db_connection = mysql.connect(
    host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
)
cursor = db_connection.cursor()
query = "use " + query_on
cursor.execute(query)

#wsadmin = Workspace(ws_url, token=ws_user_token)
#hs =  HandleService(handle_service_url, token=ws_user_token)

def get_blobstore_nodes ():
    client_blobstore = MongoClient(mongoDB_metrics_connection + to_blobstore)
    db_blobstore = client_blobstore.blobstore

    blobstore_nodes_set = set()
    blobstore_dict = dict()
    
    nodes_query = db_blobstore.nodes.find({},{"_id": 0, "id": 1, "own.user": 1, "time": 1})
    for record in nodes_query:
        blobstore_node_id = record["id"]
        user = "empty"
        if "own" in record and "user" in record["own"]:
            user = record["own"]["user"]
        save_date = record["time"]
        blobstore_nodes_set.add(blobstore_node_id)
        blobstore_dict[blobstore_node_id] = {"user": user,
                                             "date": save_date,
                                             }
    return (blobstore_nodes_set, blobstore_dict)

def get_handles_and_blobstore_ids ():
    client_handle_db = MongoClient(mongoDB_metrics_connection + to_handle_db)
    db_handle = client_handle_db.handle_db

    handles_set = set()
    handles_blobstore_ids_set = set()
    handles_by_hid_dict = dict()
    handles_by_bsid_dict = dict()
    
    handles_query = db_handle.handle.find({},{"_id": 0, "id": 1, "hid": 1, "created_by":1, "creation_date":1})
    for record in handles_query:
        blobstore_id = record["id"]
        handle = record["hid"]
        user = record["created_by"]
        save_date = record["creation_date"]
        handles_set.add(handle)
        handles_blobstore_ids_set.add(blobstore_id)
        handles_by_hid_dict[handle] = {"bsid": blobstore_id,
                                       "user": user,
                                       "date": save_date,
                                       }
        handles_by_bsid_dict[blobstore_id] = {"handle" : handle,
                                              "user": user,
                                              "date": save_date,
                                              }      
        
    return (handles_set, handles_blobstore_ids_set, handles_by_hid_dict, handles_by_bsid_dict)

def get_workspace_handles ():
    workspace_handles_set = set()
    workspace_dict = dict()
    ws_obj_vers_cursor = db.workspaceObjVersions.find(
        {#"ws":312,
            "extids.handle" : { "$exists": True }},
        {
            "type": 1,
            "ws": 1, 
            "id": 1,
            "ver": 1,
            "extids": 1,
            "savedate": 1,
            "savedby": 1,
            "_id": 0,
        },
        no_cursor_timeout=True
    )
    for ws_obj_ver in ws_obj_vers_cursor:
        obj_type = ws_obj_ver["type"]
        ws = ws_obj_ver["ws"]
        obj_id = ws_obj_ver["id"]
        ver = ws_obj_ver["ver"]
        savedate = ws_obj_ver["savedate"]
        savedby = ws_obj_ver["savedby"]
        extids = ws_obj_ver["extids"]
        handles = extids["handle"]
        full_obj_id = str(ws) + "/" + str(obj_id) + "/" + str(ver)
        for handle in handles:
            (kbh_prefix, str_handle_id) = handle.split("_")
            int_handle = int(str_handle_id)
            workspace_handles_set.add(int_handle)
            if int_handle not in workspace_dict :
                workspace_dict[int_handle] = dict()
            workspace_dict[int_handle][full_obj_id] = { "ws" : ws,
                                                        "date" : savedate,
                                                        "user" : savedby,
                                                        "type" : obj_type
                                                       }
    return (workspace_handles_set, workspace_dict)

(blobstore_nodes_set, blobstore_dict) = get_blobstore_nodes()
print("blobstore_nodes_set length : " + str(len(blobstore_nodes_set)))
(handles_set, handles_blobstore_ids_set, handles_by_hid_dict, handles_by_bsid_dict) = get_handles_and_blobstore_ids()
print("handles_set length : " + str(len(handles_set)))
print("handle_blobstore_ids_set length : " + str(len(handles_blobstore_ids_set)))
(workspace_handles_set, workspaces_dict) = get_workspace_handles()
print("workspace_handles_set length : " + str(len(workspace_handles_set)))

blobstore_nodes_not_in_handles_set = blobstore_nodes_set.difference(handles_blobstore_ids_set)
handles_blobstores_not_in_blobstore_nodes = handles_blobstore_ids_set.difference(blobstore_nodes_set)

handles_not_in_worspace_handles_set = handles_set.difference(workspace_handles_set)
workspace_handles_not_in_handles_set = workspace_handles_set.difference(handles_set)


wsov_handle_ids_not_in_handle_insert_cursor = db_connection.cursor(prepared=True)
wsov_handle_ids_not_in_handle_insert_statement = (
    "insert into metrics.wsov_handle_ids_not_in_handle "
    "(ws_obj_ver_id, save_date, ws_id, handle_id, username, type) "
    "values(%s, %s, %s, %s, %s, %s)"
)

wsov_handle_ids_not_in_handle_insert_count = 0
wsov_handle_ids_not_in_handle_skipped_insert_count = 0
for handle_id in workspace_handles_not_in_handles_set:
    for full_obj_id in workspaces_dict[handle_id]:
        ws_id = workspaces_dict[handle_id][full_obj_id]["ws"]
        save_date = workspaces_dict[handle_id][full_obj_id]["date"]
        user = workspaces_dict[handle_id][full_obj_id]["user"]
        obj_type = workspaces_dict[handle_id][full_obj_id]["type"]

        input_vals = (
            full_obj_id,
            save_date,
            ws_id,
            handle_id,
            user,
            obj_type,
        )

        try:
            wsov_handle_ids_not_in_handle_insert_cursor.execute(wsov_handle_ids_not_in_handle_insert_statement, input_vals)
            wsov_handle_ids_not_in_handle_insert_count += 1
        except mysql.Error as err:
            wsov_handle_ids_not_in_handle_skipped_insert_count += 1

#####
    
handle_ids_not_in_ws_obj_ver_insert_cursor = db_connection.cursor(prepared=True)
handle_ids_not_in_ws_obj_ver_insert_statement = (
    "insert into metrics.handle_ids_not_in_ws_obj_ver "
    "(blobstore_id, handle_id, username, save_date) "
    "values(%s, %s, %s, %s) "
)

handle_ids_not_in_ws_obj_ver_insert_count = 0
handle_ids_not_in_ws_obj_ver_skipped_insert_count = 0
for handle_id in handles_not_in_worspace_handles_set:
    bsid = handles_by_hid_dict[handle_id]["bsid"]
    user = handles_by_hid_dict[handle_id]["user"]
    if user is None:
        print("Entry for handle_id " + str(handle_id)  + " :: " + str(handles_by_hid_dict[handle_id]))
        user = "No User Found"
    save_date = handles_by_hid_dict[handle_id]["date"]
    input_vals = (
        bsid,
        handle_id,
        user,
        save_date,
        )
    try:
        handle_ids_not_in_ws_obj_ver_insert_cursor.execute(handle_ids_not_in_ws_obj_ver_insert_statement, input_vals)
        handle_ids_not_in_ws_obj_ver_insert_count += 1
    except mysql.Error as err:
        handle_ids_not_in_ws_obj_ver_skipped_insert_count += 1
        
#####
        
handles_blobstore_ids_not_in_nodes_insert_cursor = db_connection.cursor(prepared=True)
handles_blobstore_ids_not_in_nodes_insert_statement = (
    "insert into metrics.handles_blobstore_ids_not_in_nodes "
    "(blobstore_id, handle_id, username, save_date) "
    "values(%s, %s, %s, %s) "
)

handles_blobstore_ids_not_in_nodes_insert_count = 0
handles_blobstore_ids_not_in_nodes_skipped_insert_count = 0
for bsid in handles_blobstores_not_in_blobstore_nodes:
    handle_id = handles_by_bsid_dict[bsid]["handle"]
    user = handles_by_bsid_dict[bsid]["user"]
    if user is None:
        print("Entry for bsid " + str(bsid)  + " :: " + str(handles_by_bsid_dict[bsid]))
        user = "No User Found"
    save_date = handles_by_bsid_dict[bsid]["date"]
    input_vals = (
        bsid,
        handle_id,
        user,
        save_date,
        )
    try:
        handles_blobstore_ids_not_in_nodes_insert_cursor.execute(handles_blobstore_ids_not_in_nodes_insert_statement, input_vals)
        handles_blobstore_ids_not_in_nodes_insert_count += 1
    except mysql.Error as err:
        handles_blobstore_ids_not_in_nodes_skipped_insert_count += 1
        
#####
        
blobstore_ids_not_in_handle_insert_cursor = db_connection.cursor(prepared=True)
blobstore_ids_not_in_handle_insert_statement = (
        "insert into metrics.blobstore_ids_not_in_handle "
        "(blobstore_id, username, save_date) "
        "values(%s, %s, %s) "
    )

blobstore_ids_not_in_handle_insert_count = 0
blobstore_ids_not_in_handle_skipped_insert_count = 0
for blobstore_id in blobstore_nodes_not_in_handles_set:
    user = blobstore_dict[blobstore_id]["user"]
    if user is None:
        print("Entry for bsid " + str(blobstore_id)  + " :: " + str(blobstore_dict[blobstore_id]))
        user = "No User Found"
    save_date = blobstore_dict[blobstore_id]["date"]
    input_vals = (
        blobstore_id,
        user,
        save_date,
        )
    try:
        blobstore_ids_not_in_handle_insert_cursor.execute(blobstore_ids_not_in_handle_insert_statement, input_vals)
        blobstore_ids_not_in_handle_insert_count += 1
    except mysql.Error as err:
        blobstore_ids_not_in_handle_skipped_insert_count += 1
    
i = 0
print("Blobstore_dict :")
for bs_id in blobstore_dict:
    i += 1
    if i > 4:
        break
    print("Blobstore : " + bs_id + " ::: " + str(blobstore_dict[bs_id]))

i = 0
print("handle_by_hid_dict :")
for hid in handles_by_hid_dict:
    i += 1
    if i > 4:
        break
    print("Handle : " + str(hid) + " ::: " + str(handles_by_hid_dict[hid]))

i = 0
print("handle_by_bsid_dict :")
for bsid in handles_by_bsid_dict:
    i += 1
    if i > 4:
        break
    print("BSID : " + str(bsid) + " ::: " + str(handles_by_bsid_dict[bsid]))

i = 0
print("workspaces_dict :")
for hid in workspaces_dict:
    i += 1
    if i > 4:
        break
    print("Handle : " + str(hid) + " ::: " + str(workspaces_dict[hid]))

print("blobstore_nodes_set length : " + str(len(blobstore_nodes_set)))
print("handle_blobstore_ids_set length : " + str(len(handles_blobstore_ids_set)))
print("handles_set length : " + str(len(handles_set)))
print("workspace_handles_set length : " + str(len(workspace_handles_set)))

print("blobstore_nodes_not_in_handles_set length : " +  str(len(blobstore_nodes_not_in_handles_set)))
print("handles_blobstores_not_in_blobstore_nodes length : " + str(len(handles_blobstores_not_in_blobstore_nodes)))
print("handles_not_in_worspace_handles_set length : " + str(len(handles_not_in_worspace_handles_set)))
print("workspace_handles_not_in_handles_set : " + str(len(workspace_handles_not_in_handles_set)))

print("wsov_handle_ids_not_in_handle_insert_count : " + str(wsov_handle_ids_not_in_handle_insert_count))
print("wsov_handle_ids_not_in_handle_skipped_insert_count : " + str(wsov_handle_ids_not_in_handle_skipped_insert_count))
print("handle_ids_not_in_ws_obj_ver_insert_count : " + str(handle_ids_not_in_ws_obj_ver_insert_count))
print("handle_ids_not_in_ws_obj_ver_skipped_insert_count : " + str(handle_ids_not_in_ws_obj_ver_skipped_insert_count))

print("handles_blobstore_ids_not_in_nodes_insert_count : " + str(handles_blobstore_ids_not_in_nodes_insert_count))
print("handles_blobstore_ids_not_in_nodes_skipped_insert_count : " + str(handles_blobstore_ids_not_in_nodes_skipped_insert_count))
print("blobstore_ids_not_in_handle_insert_count : " + str(blobstore_ids_not_in_handle_insert_count))
print("blobstore_ids_not_in_handle_skipped_insert_count : " + str(blobstore_ids_not_in_handle_skipped_insert_count))

print("--- total seconds %s seconds ---" % (time.time() - start_time))

db_connection.commit()
db_connection.close()

exit()

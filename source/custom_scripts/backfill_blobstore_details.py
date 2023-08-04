from pymongo import MongoClient
from pymongo import ReadPreference
from biokbase.workspace.client import Workspace
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

ws_url = os.environ["WS_URL"]
ws_user_token = os.environ["METRICS_WS_USER_TOKEN"]
to_workspace = os.environ["WRK_SUFFIX"]

to_blobstore = os.environ["BLOBSTORE_SUFFIX"]
to_handle_db = os.environ["HANDLE_DB_SUFFIX"]


client = MongoClient(mongoDB_metrics_connection + to_workspace)
db = client.workspace
handle_service_url = "https://kbase.us/services/handle_service"

#wsadmin = Workspace(ws_url, token=ws_user_token)
#hs =  HandleService(handle_service_url, token=ws_user_token)

def make_blobstore_lookup ():
    client_blobstore = MongoClient(mongoDB_metrics_connection + to_blobstore)
    db_blobstore = client_blobstore.blobstore

    blobstore_nodes_size_lookup = dict()
    
    nodes_query = db_blobstore.nodes.find({},{"_id": 0, "id": 1, "size": 1})
    for record in nodes_query:
        blobstore_node_id = record["id"]
        size = record["size"]
        blobstore_nodes_size_lookup[blobstore_node_id] = size
    return blobstore_nodes_size_lookup

def make_handle_id_lookup ():
    client_handle_db = MongoClient(mongoDB_metrics_connection + to_handle_db)
    db_handle = client_handle_db.handle_db

    handle_id_lookup = dict()
    
    handles_query = db_handle.handle.find({},{"_id": 0, "id": 1, "hid": 1})
    for record in handles_query:
        blobstore_node_id = record["id"]
        handle = record["hid"]
        handle_id_lookup[handle] = blobstore_node_id
    return handle_id_lookup



# object_id -> {handle=>handle, node=node, type=object_type, savedate=> sd}
objects_with_problem_nodes_with_no_size = dict()
objects_with_problem_handles_with_no_nodes = dict()

running_size_total = 0

deleted_object_with_data_found_count = 0
deleted_object_without_data_found_count = 0

#exit()


# blobstore_id => {ws_obj_id => (save_date, saver)}
blobstore_object_results = dict()

# blobstore_id =>{first_saver_ws_obj_id => blah,
#                 first_save_date = date}
#blobstore_id_first_saver = dict()

#ws_ids = [146324]  # small
#ws_ids = [28129]  # fungal phytosome s
#ws_ids = [146324,28129]  # fungal phytosome and small ws, took 203 mins
#ws_ids = [19217]  # refseq reference



#for ws_id in ws_ids:
deleted_objects = set()
ws_obj_deleted_cursor = db.workspaceObjects.find({"del":True},{"_id":0, "ws": 1,"id":1})
for ws_obj_deleted in ws_obj_deleted_cursor:
    deleted_temp_ws_id = ws_obj_deleted["ws"]
    deleted_obj_id = ws_obj_deleted["id"]
    deleted_ws_obj_id = str(deleted_temp_ws_id) + "/" + str(deleted_obj_id)
    deleted_objects.add(deleted_ws_obj_id)

print("TOTAL DELETED OBJECT LENGTH: " + str(len(deleted_objects)))    
print("--- total time for the deleted objects lookup  %s seconds ---" % (time.time() - start_time))

ws_obj_vers_cursor = db.workspaceObjVersions.find(
    {#"ws":312,
     "extids.handle" : { "$exists": True }},
    {
        "type": 1,
        "ws": 1, 
        "id": 1,
        "ver": 1,
        "savedate": 1,
        "savedby": 1,
        "extids": 1,
        "_id": 0,
    },
    no_cursor_timeout=True
    )
i = 0
ws_obj_info = dict()
deleted_ext_ids_counter = 0

for ws_obj_ver in ws_obj_vers_cursor:
    is_deleted = 0
    object_type_full = ws_obj_ver["type"]
    (object_type, object_spec_version) = object_type_full.split("-")
    #if (object_type != "KBaseNarrative.Narrative" and object_type != "KBaseReport.Report"):
    ws_id = ws_obj_ver["ws"]
    obj_id = ws_obj_ver["id"]
    temp_ws_obj_id = str(ws_id) + "/" + str(obj_id)
    if temp_ws_obj_id in deleted_objects:
        deleted_ext_ids_counter += 1
        is_deleted = 1
#        continue
    obj_ver = ws_obj_ver["ver"]
    obj_save_date = ws_obj_ver["savedate"]
    savedby = ws_obj_ver["savedby"]
    extids = ws_obj_ver["extids"]
    handles = extids["handle"]    
#        for handle in handles:
#            handles_set.add(handle)
#        obj_copied = 0
    full_obj_id = str(ws_id) + "/" + str(obj_id) + "/" + str(obj_ver)
#    print("Full obj id : " + full_obj_id)
#    print("Object Type : " + object_type_full)
#        if (object_type != "KBaseNarrative.Narrative" and object_type != "KBaseReport.Report"):
#        if (object_type == "KBaseNarrative.Narrative" or object_type == "KBaseReport.Report"):

    ws_obj_info[full_obj_id] = {"save_date" : obj_save_date,
                                "savedby" : savedby,
                                "obj_type" : object_type_full,
                                "handles" : handles,
                                "is_deleted" : is_deleted}

print("--- total time for the ws_object_version objects query  %s seconds ---" % (time.time() - start_time))
    
##########################################################################
print("BLOBSTORE LOOKUP:")
blobstore_lookup = make_blobstore_lookup()
test_counter = 0
for temp_key in blobstore_lookup:
    if test_counter < 10:
        print("ID: " + str(temp_key) + "   :::   size: " + str(blobstore_lookup[temp_key]))
    else:
        break
    test_counter = test_counter + 1
print("Total BLOBSTORE Lookuplength: " + str(len(blobstore_lookup)))

print("--- total time for the blobstore size lookup creation  %s seconds ---" % (time.time() - start_time))

handle_id_lookup = make_handle_id_lookup()
test_counter = 0
for temp_key in handle_id_lookup:
    if test_counter < 10:
        print("ID: " + str(temp_key) + "   :::   blobstore_id: " + str(handle_id_lookup[temp_key]))
    else:
        break
    test_counter = test_counter + 1
print("Total HANDLE ID lookup length: " + str(len(handle_id_lookup)))

print("--- total time for the blobstore size lookup creation  %s seconds ---" % (time.time() - start_time))
##############################################    

for full_obj_id in  ws_obj_info:
#    print("ws_obj_info[full_obj_id][handles] : " + str(ws_obj_info[full_obj_id]["handles"]))
    for handle in ws_obj_info[full_obj_id]["handles"]:
        blobstore_id = None
        (kbh_prefix, str_handle_id) = handle.split("_")
        if int(str_handle_id) in handle_id_lookup:
            blobstore_id = handle_id_lookup[int(str_handle_id)]
        else:
            objects_with_problem_handles_with_no_nodes[full_obj_id] = ws_obj_info[full_obj_id]
            if ws_obj_info[full_obj_id]["is_deleted"] == 1:
                deleted_object_without_data_found_count += 1

        if blobstore_id and blobstore_id in blobstore_lookup:
            if blobstore_id not in blobstore_object_results:
                blobstore_object_results[blobstore_id] = dict()
            blobstore_object_results[blobstore_id][full_obj_id] = (ws_obj_info[full_obj_id]["save_date"],
                                                                   ws_obj_info[full_obj_id]["savedby"])
#            print("Blobstore lookup file_size : " + str(blobstore_lookup[blobstore_id]))
            if ws_obj_info[full_obj_id]["is_deleted"] == 1:
                deleted_object_with_data_found_count += 1
            file_size = blobstore_lookup[blobstore_id]
            running_size_total = running_size_total + file_size
        else:
#            print("HUGE PROBLEM: obj_id : " + full_obj_id +  " blobstore_id: "  + str(blobstore_id) + " IS NOT IN THE LOOKUP")
#            del blobstore_object_results[blobstore_id]
            objects_with_problem_nodes_with_no_size[full_obj_id] = ws_obj_info[full_obj_id]
            if ws_obj_info[full_obj_id]["is_deleted"] == 1:
                deleted_object_without_data_found_count += 1

print("objects_with_problem_nodes_with_no_size : " + str(objects_with_problem_nodes_with_no_size))            
print("TOTAL objects_with_problem_nodes_with_no_size : " + str(len(objects_with_problem_nodes_with_no_size)))

print("objects_with_problem_handles_with_no_nodes : " + str(objects_with_problem_handles_with_no_nodes))
print("TOTAL objects_with_problem_handles_with_no_nodes : " + str(len(objects_with_problem_handles_with_no_nodes)))

print("deleted_object_with_data_found_count :" + str(deleted_object_with_data_found_count))
print("deleted_object_without_data_found_count :" + str(deleted_object_without_data_found_count))

print("blobstore_object_results length :  " +  str(len(blobstore_object_results)))
#print("blobstore_object_results :  " +  str(blobstore_object_results))
print("RUNNING TOTAL SIZE : " + str(running_size_total))

obj_id_set = set()
for blobstore_id in blobstore_object_results :
    for obj_id in  blobstore_object_results[blobstore_id]:
        obj_id_set.add(obj_id)
print("Total number of objects with handles that could be fully determined : " + str(len(obj_id_set)))

print("Total ext_ids objects that were deleted : " + str(deleted_ext_ids_counter))

#print("blobstore_object_results : " + str(blobstore_object_results))
    
print("--- total seconds %s seconds ---" % (time.time() - start_time))



exit()

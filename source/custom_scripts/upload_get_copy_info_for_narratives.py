#import pymongo
from pymongo import MongoClient
from pymongo import ReadPreference
#from biokbase.workspace.client import Workspace
#from installed_clients.AbstractHandleClient import AbstractHandle as HandleService
#from biokbase.service.Client import Client as ServiceClient
#import json as _json
import os
import mysql.connector as mysql
import requests
import time
#from splitting import split_sequence
from datetime import date
from datetime import datetime

debug_mode = 1

if debug_mode == 1:
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

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
sql_host = os.environ["SQL_HOST"]
metrics = os.environ["QUERY_ON"]

# connect to mysql
db_connection = mysql.connect(
    host=sql_host,  # "mysql1", #"localhost",
    user="metrics",  # "root",
    passwd=metrics_mysql_password,
    database="metrics",  # "datacamp"
)

cursor = db_connection.cursor()
query = "use " + metrics
cursor.execute(query)

client = MongoClient(mongoDB_metrics_connection + to_workspace)
db = client.workspace

workspaces_without_corresponding_versions_data = list()

#workspaces_with_multiple_narrative_obj_ids = dict()

##############################
#
# get the list of narrative_typed objects
#
############################
def get_narrative_typed_objects_list(cursor):
    # get list of narrative typed objects on the system
    query = ('select object_type, object_type_full from metrics_reporting.workspace_object_counts_current where object_type like "KBaseNarrative.Narrative%"')
    cursor.execute(query)
    narrative_type_list = list()
    for row_values in cursor:
        narrative_type_list.append(row_values[1])
    return narrative_type_list
    
###############################
#
#   Get a dict of Workspaces that contain a narrative - with its corresponding info
#     {key: ws_id => {"id" => Object ID of the version 1 of the narratove,
#                     "savedate" => date that version 1 of the narrative was created.
#
###############################
def get_ws_narratives(db):
#def get_ws_narratives(db, narrative_type_list):
    ws_narratives_dict = dict()
#    workspaces_with_multiple_narrative_obj_ids = dict()
    narrative_obj_ids_not_int_dict = dict() #key ws -> value the narrative value
    narrative_obj_ids_not_int_never_resolved = dict() #key ws -> value the narrative value

    meta_without_narrative_count = 0
    meta_with_multiple_narratives_count = 0

    workspaces_with_meta_cursor = db.workspaces.find({"meta" : {"$exists": True}},{"ws":1,"meta":1})
    for workspace_with_meta in workspaces_with_meta_cursor:
        narrative_ws_id = workspace_with_meta["ws"]
#        if narrative_ws_id != 100417:
#            continue
        meta_narrative = None
#        print("narrative_ws_id : " + str(narrative_ws_id))
#        print("  workspace_with_meta meta : " + str(  workspace_with_meta["meta"]))
        for meta_element in workspace_with_meta["meta"]:
#            print("    meta_element : " + str(meta_element))
            if "narrative" == meta_element["k"]:
#                print("narrative in meta element")
                if meta_narrative is None:
                    meta_narrative = meta_element["v"]
                else:
                    if meta_narrative != meta_element["v"]:
                        meta_with_multiple_narratives_count += 1
#                        print("  workspace_with_meta multiple narratives : " + str(  workspace_with_meta["meta"]))
        if meta_narrative is None:
            meta_without_narrative_count += 1
        else:
            ws_narratives_dict[narrative_ws_id] = dict()
            try:
                ws_narratives_dict[narrative_ws_id]["id"] = int(meta_narrative)
            except ValueError:
                del(ws_narratives_dict[narrative_ws_id])
                narrative_obj_ids_not_int_dict[narrative_ws_id] = meta_narrative
    #NOW NEED TO RESOLVE THE narrative id indicator that is not an integer:
    for narrative_obj_id_not_int in narrative_obj_ids_not_int_dict:
#        print("narrative_obj_id_not_int : " + str(narrative_obj_id_not_int))
#        print("narrative_obj_ids_not_int_dict[narrative_obj_id_not_int] : " + str(narrative_obj_ids_not_int_dict[narrative_obj_id_not_int]))
        workspaceObjectsName_cursor = db.workspaceObjects.find({"ws": narrative_obj_id_not_int,
                                                                "name": narrative_obj_ids_not_int_dict[narrative_obj_id_not_int]},
                                                               {"ws":1,"id":1})
        record_found = 0
        for workspaceObjectsName in workspaceObjectsName_cursor:
            record_found = 1
            ws_narratives_dict[narrative_obj_id_not_int] = dict()
            ws_narratives_dict[narrative_obj_id_not_int]["id"] = workspaceObjectsName["id"]
        if record_found == 0:
            narrative_obj_ids_not_int_never_resolved[narrative_obj_id_not_int] = narrative_obj_ids_not_int_dict[narrative_obj_id_not_int]
            narrative_obj_id_not_int
    
    print("meta_without_narrative_count : " + str(meta_without_narrative_count))
    print("meta_with_multiple_narratives_count : " + str(meta_with_multiple_narratives_count))
    print("ws_narratives_dict_length : " + str(len(ws_narratives_dict)))
    print("narrative_obj_ids_not_int_never_resolved : " + str(narrative_obj_ids_not_int_never_resolved))
    print("narrative_obj_ids_not_int_never_resolved length : " + str(len(narrative_obj_ids_not_int_never_resolved)))
    print("ws_narratives_dict length 1 : " + str(len(ws_narratives_dict)))

#    exit()

    processed_narratives_count = 0
    test_ws_narratives_dict = dict()

    # NOW DETERMINE THE SAVEDATE
    for narrative_ws_id in ws_narratives_dict:
        processed_narratives_count += 1
#        if processed_narratives_count < 140000:
#            continue
#        test_list = [ 13129,59769,56261,100417 ]
#        if narrative_ws_id in test_list:
        # NOW GET THE SAVE DATE FOR THE FIRST NARRATIVE VERSION
#        print("Narrative ws id : " + str(narrative_ws_id))
#        obj_id = ws_narratives_dict[narrative_ws_id]["id"]
#        print("id : " + str(ws_narratives_dict[narrative_ws_id]["id"]))
        get_narrative_savedate_cursor = db.workspaceObjVersions.find({"ws": narrative_ws_id, "id":ws_narratives_dict[narrative_ws_id]["id"], "ver":1},{"ws":1, "id":1, "savedate":1, "_id":0})
        found_object_ver = 0
        for narrative_savedate_record in get_narrative_savedate_cursor:
            ws_narratives_dict[narrative_ws_id]["savedate"] = narrative_savedate_record["savedate"]
            found_object_ver = 1
#            test_ws_narratives_dict[narrative_ws_id] = ws_narratives_dict[narrative_ws_id]
        if found_object_ver == 0:
            workspaces_without_corresponding_versions_data.append(narrative_ws_id)
        if processed_narratives_count % 1000 == 0:
            print("Processed savedate for : " + str(processed_narratives_count) + " narratives")
#    print("test_ws_narratives_dict : " + str(test_ws_narratives_dict))
#    print("test_ws_narratives_dict length 2: " + str(len(test_ws_narratives_dict)))
#    print("test_ws_narratives_dict : " + str(test_ws_narratives_dict))

    for ws_id_to_delete in workspaces_without_corresponding_versions_data:
        del(ws_narratives_dict[ws_id_to_delete])
    print("ws_narratives_dict length 2: " + str(len(ws_narratives_dict)))

    return ws_narratives_dict


#############################
#
#   Determine if the narrative was created from a copied operation
#   Grab all ws_obj_versions that have a savedate <= the savedate of the first version of the narratoive object
#   If all those objects have copied and from the same source WS, all are version 1, and all have a lower object id than the narrative object.
#       Then it was copied from that WS.  Now determine which version of that narrative was it copied from.
#   Then look at versions of source narrative and take correct one with max date that is less than destination narrative savedate
#
#############################
def determine_if_narratives_are_copies(db, ws_narratives_dict, narrative_type_list):
    ws_that_were_narrative_copy_list = list()
    copied_ws_narratives_dict = dict()
    source_ws_id_to_copied_ws_ids = dict()

    multiple_workspace_source_count = 0
    multiple_workspace_source_set = set()
    fresh_narrative_count = 0
    not_all_pre_objects_copied_count = 0
    not_all_pre_objects_copied_set = set()
    final_else_count = 0
    
#    temp_ws_narratives_dict = dict()
#    temp_ws_narratives_dict[103334] = ws_narratives_dict[103334]
#    ws_narratives_dict = temp_ws_narratives_dict

    print("ws_narratives_dict length : " + str(len(ws_narratives_dict)))

    for potential_narrative_ws in sorted(ws_narratives_dict):
        objects_to_check_count = 0
        objects_copied_count = 0
        workspace_ids_copied_from_set = set()
        print("potential_narrative_ws : " + str(potential_narrative_ws) + " Dict: " + str(ws_narratives_dict[potential_narrative_ws]))
        object_to_check_cursor = db.workspaceObjVersions.find({"savedate":{"$lt":ws_narratives_dict[potential_narrative_ws]["savedate"]},
                                                               "ws":potential_narrative_ws},
                                                              {"ws":1, "id":1, "copied":1,"savedate":1, "ver":1, "type":1, "_id":0});
        
        for object_to_check in object_to_check_cursor:
            object_type = object_to_check["type"]
            if object_type in narrative_type_list:
                # skip narrative objects
                continue
            copied_from = object_to_check["copied"]
#            print("copied_from : " + str(copied_from))
            if copied_from is not None:
                source_ws = int(copied_from.split("/")[0])
#                if objects_copied_count == 2:
#                    source_ws = 111
                workspace_ids_copied_from_set.add(source_ws)
                objects_copied_count += 1
            objects_to_check_count += 1
        if objects_copied_count > 0 and (objects_to_check_count == objects_copied_count) and (len(workspace_ids_copied_from_set) == 1):
            copied_ws_narratives_dict[potential_narrative_ws] = ws_narratives_dict[potential_narrative_ws]
            source_ws_id = list(workspace_ids_copied_from_set)[0]
            copied_ws_narratives_dict[potential_narrative_ws]["copied_from"] = source_ws_id 
            if source_ws_id not in source_ws_id_to_copied_ws_ids:
                source_ws_id_to_copied_ws_ids[source_ws_id] = list()
            source_ws_id_to_copied_ws_ids[source_ws_id].append(potential_narrative_ws)
#            print("IT WAS COPIED : WS : " + str(potential_narrative_ws) + " copied from : " + str(workspace_ids_copied_from_set))
        elif len(workspace_ids_copied_from_set) > 1:
#            print("NOT COPIED FROM ONE WS : " + str(workspace_ids_copied_from_set))
            multiple_workspace_source_count += 1
            multiple_workspace_source_set.add(potential_narrative_ws)
        elif objects_copied_count == 0:
#            print("This was a fresh narrative")
            fresh_narrative_count += 1                                
        elif objects_copied_count != objects_to_check_count:
#            print("Not all objectswere copied")
            not_all_pre_objects_copied_count += 1
            not_all_pre_objects_copied_set.add(potential_narrative_ws)
        else:
#            print("Should hopefully never get here")
            final_else_count += 1
        print("Processed ws : " + str(potential_narrative_ws))
#    print("copied_ws_narratives_dict : " + str(copied_ws_narratives_dict))



    print("multiple_workspace_source_count : " + str(multiple_workspace_source_count))
    print("multiple_workspace_source_set : " + str(sorted(multiple_workspace_source_set)))
    print("fresh_narrative_count : " + str(fresh_narrative_count))
    print("not_all_pre_objects_copied_count : " + str(not_all_pre_objects_copied_count))
    print("not_all_pre_objects_copied_set : " + str(sorted(not_all_pre_objects_copied_set)))
    print("final_else_count : " + str(final_else_count))

#    multiple_workspace_source_in_multi_narrative_count = 0
#    for temp_ws_id in multiple_workspace_source_set:
#        if temp_ws_id in workspaces_with_multiple_narrative_obj_ids:
#            multiple_workspace_source_in_multi_narrative_count += 1
#    print("multiple_workspace_source_in_multi_narrative_count : " + str(multiple_workspace_source_in_multi_narrative_count))
            
#    not_all_pre_objects_copied_in_multi_narrative_count = 0
#    for temp_ws_id in not_all_pre_objects_copied_set:
#        if temp_ws_id in workspaces_with_multiple_narrative_obj_ids:
#            not_all_pre_objects_copied_in_multi_narrative_count += 1
#    print("not_all_pre_objects_copied_in_multi_narrative_count : " + str(not_all_pre_objects_copied_in_multi_narrative_count))
            
    return (copied_ws_narratives_dict,source_ws_id_to_copied_ws_ids)

def determine_source_narrative_version(db, copied_ws_narratives_dict, source_ws_id_to_copied_ws_ids, narrative_type_list):
    destination_upa_from_source_upa_dict = dict()
    returned_copied_ws_narratives_dict = dict()
    unable_to_find_source_upa = list()
    for source_ws_id in source_ws_id_to_copied_ws_ids:
        ordered_save_points = list()
        source_version_save_points_cursor = db.workspaceObjVersions.find({"type":{"$in":narrative_type_list},
                                                                          "ws":source_ws_id},
                                                                         {"id":1, "ver":1, "savedate":1, "_id":0}).sort("savedate")
        for source_version_save_point in source_version_save_points_cursor:
            source_obj_id = str(source_ws_id) + "/" + str(source_version_save_point["id"]) + "/" + str(source_version_save_point["ver"])
            savedate = source_version_save_point["savedate"]
            ordered_save_points.append([savedate,source_obj_id])

        for destination_ws_id in source_ws_id_to_copied_ws_ids[source_ws_id]:
            destination_ws_savedate = copied_ws_narratives_dict[destination_ws_id]["savedate"]
            source_obj_id_used = None
            for ordered_save_point in ordered_save_points:
                if ordered_save_point[0] <= destination_ws_savedate:
                    source_obj_id_used = ordered_save_point[1]
                else:
                    break
            if source_obj_id_used == None:
                unable_to_find_source_upa.append(destination_ws_id)
            else:
                destination_upa = str(destination_ws_id) + "/" + str(copied_ws_narratives_dict[destination_ws_id]["id"]) + "/1"
                destination_upa_from_source_upa_dict[destination_upa] = source_obj_id_used
                returned_copied_ws_narratives_dict[destination_ws_id] = copied_ws_narratives_dict[destination_ws_id]
                returned_copied_ws_narratives_dict[destination_ws_id]["destination_narrative_upa"] = destination_upa
                returned_copied_ws_narratives_dict[destination_ws_id]["source_narrative_upa"] = source_obj_id_used
    return (destination_upa_from_source_upa_dict,returned_copied_ws_narratives_dict)

def upload_past_narrative_copies(returned_copied_ws_narratives_dict):
    prep_cursor = db_connection.cursor(prepared=True)
    past_narrative_copies_insert_statement = (
        "insert into past_narrative_copies "
        "(source_narrative_id, source_narrative_upa, destination_narrative_id, destination_narrative_upa, destination_narrative_save_date) "
        "values(%s, %s, %s, %s, %s);")
    for copied_narrative_ws_id in returned_copied_ws_narratives_dict:
        input = (returned_copied_ws_narratives_dict[copied_narrative_ws_id]['copied_from'],
                    returned_copied_ws_narratives_dict[copied_narrative_ws_id]['source_narrative_upa'],
                    copied_narrative_ws_id,
                    returned_copied_ws_narratives_dict[copied_narrative_ws_id]['destination_narrative_upa'],
                    returned_copied_ws_narratives_dict[copied_narrative_ws_id]['savedate'])
        prep_cursor.execute(past_narrative_copies_insert_statement, input)
    db_connection.commit()

narrative_type_list = get_narrative_typed_objects_list(cursor)
#ws_narratives_dict = get_ws_narratives(db, narrative_type_list)
ws_narratives_dict = get_ws_narratives(db)
print("ws_narratives_dict length : " + str(len(ws_narratives_dict)))

# NEED TO CODE UP AND WS ADMISNISTER TO DO AND THEN REPOPULATE WS_NARRATIVES DICT WITH THE PROPER NARRATIVE
# SEE methods_upload_workspace_stats line 337 to 339.
#ws_narratives_dict = cleanup_multiple_narrative_object_ids(db, ws_narratives_dict, workspaces_with_multiple_narrative_obj_ids)
(copied_ws_narratives_dict,source_ws_id_to_copied_ws_ids) = determine_if_narratives_are_copies(db, ws_narratives_dict, narrative_type_list)
(destination_upa_from_source_upa_dict,returned_copied_ws_narratives_dict) = determine_source_narrative_version(db, copied_ws_narratives_dict, source_ws_id_to_copied_ws_ids, narrative_type_list)
upload_past_narrative_copies(returned_copied_ws_narratives_dict)

print("copied_ws_narratives_dict length : " + str(len(copied_ws_narratives_dict)))
print("source_ws_id_to_copied_ws_ids length : " + str(len(source_ws_id_to_copied_ws_ids)))
print("destination_upa_from_source_upa_dict length : " + str(len(destination_upa_from_source_upa_dict)))
print("workspaces_without_corresponding_versions_data : " + str(workspaces_without_corresponding_versions_data))
print("workspaces_without_corresponding_versions_data length : " + str(len(workspaces_without_corresponding_versions_data)))

i = 0
for destination_upa in destination_upa_from_source_upa_dict :
    if i < 5:                                                                                   
        print(destination_upa + "\t" +  destination_upa_from_source_upa_dict[destination_upa])
    else:
        break
    i += 1

print("returned_copied_ws_narratives_dict examples:")
i = 0
for copied_narrative_ws_id in returned_copied_ws_narratives_dict:
    if i < 5:
        print(str(copied_narrative_ws_id) + "\t" +  str(returned_copied_ws_narratives_dict[copied_narrative_ws_id]))
    else:
        break
    i += 1

# loop through each of the sources, get all versions timestamps
# THen determine which version of the source for each distination copy event




##################
#
# Still need to do determination of which source narrative version.
#
# Need to do a reverse lookup object source_narrative_id -> [list of destination narratives]
#
#####################

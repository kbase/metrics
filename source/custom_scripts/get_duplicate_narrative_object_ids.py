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

ws_url = os.environ["WS_URL"]
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

########################3
#
#   get_ws_narratives with duplicate narrative_ids
#
#####################
def get_multiple_narratives_count_dict(cursor):
    # get list of narrative typed objects on the system
    query = ('select ws_id, num_nar_obj_ids  from metrics_reporting.workspaces_current where num_nar_obj_ids > 1')
    cursor.execute(query)
    multiple_narrative_count_dict = dict()
    for row_values in cursor:
        multiple_narrative_count_dict[row_values[0]] = row_values[1]
#    print(" multiple_narrative_count_dict : " + str(multiple_narrative_count_dict))
#    print(" multiple_narrative_count_dict length : " + str(len(multiple_narrative_count_dict)))
    return multiple_narrative_count_dict

####################
#
#   get active narrative for all of these workspaces (note may have by name)
#   Then get the list of all non_active obj_ids for these narratives
#   Confirm the length of each list is n-1 relative to the count list
#
##################
def get_non_active_narrative_object_ids(narrative_type_list, multiple_narrative_count_dict, db):
    narrative_active_id_dict = dict()
    list_of_workspace_to_check = list(multiple_narrative_count_dict.keys())
#    print("list_of_workspace_to_check len : " + str(len(list_of_workspace_to_check)))
    
    ws_narratives_dict = dict()
#    
    narrative_obj_ids_not_int_dict = dict() #key ws -> value the narrative value
#
    narrative_obj_ids_not_int_never_resolved = dict() #key ws -> value the narrative value
#
    meta_without_narrative_count = 0
#
    meta_with_multiple_narratives_count = 0

    workspaces_with_meta_cursor = db.workspaces.find({"meta" : {"$exists": True}, "ws" : {"$in":list_of_workspace_to_check}},{"ws":1,"meta":1})
    workspaces_with_meta_cursor_count = 0
    for workspace_with_meta in workspaces_with_meta_cursor:
        workspaces_with_meta_cursor_count += 1
        narrative_ws_id = workspace_with_meta["ws"]
        meta_narrative = None
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
            try:
                narrative_active_id_dict[narrative_ws_id] = int(meta_narrative)
            except ValueError:
#                del(narrative_active_id_dict[narrative_ws_id])
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
            narrative_active_id_dict[narrative_obj_id_not_int] = workspaceObjectsName["id"]
        if record_found == 0:
            narrative_obj_ids_not_int_never_resolved[narrative_obj_id_not_int] = narrative_obj_ids_not_int_dict[narrative_obj_id_not_int]

#    print("workspaces_with_meta_cursor count : " + str(workspaces_with_meta_cursor_count))
#    print("meta_without_narrative_count : " + str(meta_without_narrative_count))
#    print("meta_with_multiple_narratives_count : " + str(meta_with_multiple_narratives_count))
#    print("narrative_obj_ids_not_int_never_resolved : " + str(narrative_obj_ids_not_int_never_resolved))
#    print("narrative_obj_ids_not_int_never_resolved length : " + str(len(narrative_obj_ids_not_int_never_resolved)))
#    print("narrative_active_id_dict length  : " + str(len(narrative_active_id_dict)))
#    print("narrative_active_id_dict  : " + str(narrative_active_id_dict))
#    print("narrative_type_list : " + str(narrative_type_list))

#    exit()

    # key narrative id -> value comma delimited string of non_active_ids
    return_non_active_ids_dict = dict()
    
    for narrative_with_active_id in narrative_active_id_dict:
        # now determine which obj_ids are non-active narrative objects.
        # confirm the number gotten back metches the count in  (multiple_narrative_count_dict - 1)
        non_active_narrative_ids_set = set()
        narrative_obj_ids_cursor = db.workspaceObjVersions.find({ "ws": narrative_with_active_id, "type" : {"$in":narrative_type_list}},{"id":1, "ws":1, "_id":0})
        for narrative_obj_ids_row in narrative_obj_ids_cursor:
            narrative_obj_id = narrative_obj_ids_row["id"]
#            print("narrative_obj_id : " + str(narrative_obj_id))
            if narrative_obj_id != narrative_active_id_dict[narrative_with_active_id] :
                non_active_narrative_ids_set.add(narrative_obj_id)
        if len(non_active_narrative_ids_set) != (multiple_narrative_count_dict[narrative_with_active_id] - 1):
            print("narrative_with_active_id : " + str(narrative_with_active_id) + " has a length of non_actives of " + str(len(non_active_narrative_ids_set)) +
                  " but the multiple_narrative_count_dict has a value of : " + str(multiple_narrative_count_dict[narrative_with_active_id]) +
                  " here are the non actives : " + str(non_active_narrative_ids_set))
        else:
            return_non_active_ids_dict[narrative_with_active_id] = ",".join(str(x) for x in list(non_active_narrative_ids_set))

    for return_non_active_id in return_non_active_ids_dict:
        print(str(return_non_active_id) + "\t" + return_non_active_ids_dict[return_non_active_id])
#    print("return_non_active_ids_dict : " + str(return_non_active_ids_dict))
 

narrative_type_list = get_narrative_typed_objects_list(cursor)
multiple_narrative_count_dict = get_multiple_narratives_count_dict(cursor)
get_non_active_narrative_object_ids(narrative_type_list, multiple_narrative_count_dict, db)



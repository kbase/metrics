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

workspaces_with_copied_reports_and_no_narratives = list()

client = MongoClient(mongoDB_metrics_connection + to_workspace)
db = client.workspace

# dict soucce_ws => {destination_ws => min_savedate (MIGHT NEED NARRATIVE OBJECT NUMBER)
source_ws_to_destination_ws_dict = dict()
destination_ws_set = set()

# Key destination ws_id , key = object id of the narrative
destination_narrative_obj_id_lookup = dict() 

# Final results object;
# Key = narrative_obj_id , value = ws_obj_version of the source ws object: 
destination_results_dict = dict()

# get unique list of Report types:
query = ('select object_type, object_type_full from metrics_reporting.workspace_object_counts_current where object_type like "KBaseReport.Report%"')

cursor.execute(query)
row_values = list()

report_list = list()
for row_values in cursor:
    report_list.append(row_values[1])

# GET THE INITIAL INFORMATION ABOUT COPIED REPORTS TO EXTRAPOLATE COPIED NARRATIVES:
ws_objVersions_copied_reports_cursor = db.workspaceObjVersions.find({"type":{"$in":report_list},
                                                                     "copied":{"$ne": None}
                                                                     #, "ws":{"$in":[145373, 43266, 116952, 154109]}
                                                                     },
                                                                    {"ws": 1, "_id": 0, "savedate": 1, "copied" : 1 })

for ws_objVersions_copied_report in ws_objVersions_copied_reports_cursor:
    destination_ws =  ws_objVersions_copied_report["ws"]
    savedate = ws_objVersions_copied_report["savedate"]
    copied_from = ws_objVersions_copied_report["copied"]
    source_ws = int(copied_from.split("/")[0])
    destination_ws_set.add(destination_ws)
    if source_ws not in source_ws_to_destination_ws_dict:
        source_ws_to_destination_ws_dict[source_ws] = dict()
    if destination_ws not in source_ws_to_destination_ws_dict[source_ws]:
        source_ws_to_destination_ws_dict[source_ws][destination_ws] = dict()
    if "creation_date" not in source_ws_to_destination_ws_dict[source_ws][destination_ws]:
        source_ws_to_destination_ws_dict[source_ws][destination_ws]["creation_date"] = savedate
    else:
        if savedate < source_ws_to_destination_ws_dict[source_ws][destination_ws]["creation_date"]:
            source_ws_to_destination_ws_dict[source_ws][destination_ws]["creation_date"] = savedate

if debug_mode == 1:
    print("source_ws_to_destination_ws_dict: " + str(source_ws_to_destination_ws_dict))
#
#split the copy get source WS, fill in Datastructure, replace the min_date accordingly.
 #   
  #  

# GET THE DESTINATION WS NARRATIVE OBJECT ID
# Has the obj id (middlw part of UPA) of the narrative obj in the new WS. Copied narratives are not object 1, but rather
# the max object id in source ws (at time of the copy)  + 1
destination_narratives_ids_lookup = dict()

#get narrative typed objects
query = ('select object_type, object_type_full from metrics_reporting.workspace_object_counts_current where object_type like "KBaseNarrative.Narrative%"')

cursor.execute(query)
row_values = list()

narrative_type_list = list()
for row_values in cursor:
    narrative_type_list.append(row_values[1])
  
destination_narrative_ids_cursor = db.workspaceObjVersions.find({"type":{"$in":narrative_type_list},
                                                                 "ws":{"$in":list(destination_ws_set)},
                                                                 "ver":1},
                                                                {"ws":1, "id":1, "_id":0})
  
for dest_narrative_ws_id in destination_narrative_ids_cursor:
    destination_narrative_obj_id_lookup[dest_narrative_ws_id["ws"]] = dest_narrative_ws_id["id"]

if debug_mode == 1:
    print("destination_narrative_obj_id_lookup : " + str(destination_narrative_obj_id_lookup))


# GET THE COPIED FROM NARRATIVES TIMESTAMPS OF THEIR VERSIONS TO HAVE A LOOKUP FOR THE
for source_ws_id in source_ws_to_destination_ws_dict:
    ordered_save_points = list()
    source_version_save_points_cursor = db.workspaceObjVersions.find({"type":"KBaseNarrative.Narrative-4.0",
                                                                      "ws":source_ws_id},
                                                                     {"id":1, "ver":1, "savedate":1, "_id":0}).sort("savedate")
    for source_version_save_point in source_version_save_points_cursor:
        source_obj_id = str(source_ws_id) + "/" + str(source_version_save_point["id"]) + "/" + str(source_version_save_point["ver"])
        savedate = source_version_save_point["savedate"]
        ordered_save_points.append([savedate,source_obj_id])
    if debug_mode == 1:
        print("ordered_save_points : " + str(ordered_save_points))

    for destination_ws_id in source_ws_to_destination_ws_dict[source_ws_id]:
        destination_ws_savedate = source_ws_to_destination_ws_dict[source_ws_id][destination_ws_id]["creation_date"]
        source_obj_id_used = None
        for ordered_save_point in ordered_save_points:
            if ordered_save_point[0] <= destination_ws_savedate:
                source_obj_id_used = ordered_save_point[1]
            else:
                break
        if source_obj_id_used == None:
            if debug_mode == 1:
                print("ERROR: " + str(destination_ws_id) + " does not a source ws_obj that it found, could be due to saved REPORT indipendently")
        if destination_ws_id not in destination_narrative_obj_id_lookup:
            if debug_mode == 1:
                print("It is a WS without a narrative object")
            workspaces_with_copied_reports_and_no_narratives.append(destination_ws_id)
            continue
        destination_narrative_obj_id = str(destination_ws_id) + "/" + str(destination_narrative_obj_id_lookup[destination_ws_id]) + "/1"
        destination_results_dict[destination_narrative_obj_id] = source_obj_id_used

if debug_mode == 1:
    print("destination_results_dict : " + str(destination_results_dict))
    print("===============================")
    print("===============================")
    print("===============================")

destination_obj_id_is_none = list()

narrative_copy_count = 0
print("Destination_WS\tSource_WS")
for destination_obj_id in destination_results_dict:
    if destination_results_dict[destination_obj_id] == None:
        destination_obj_id_is_none.append(destination_obj_id)
        continue
    print(destination_obj_id + "\t" + destination_results_dict[destination_obj_id])
    narrative_copy_count += 1
    
if debug_mode == 1:
    print("DESTINATION WORKSPACES HAVE NO NARRATIVE workspaces_with_copied_reports_and_no_narratives : " + str(workspaces_with_copied_reports_and_no_narratives))
    print("workspaces_with_copied_reports_and_no_narratives length " + str(len(workspaces_with_copied_reports_and_no_narratives)))

    print("SOURCE WS DOES NOT HAVE A NARRATIVE::::::::destination_obj_id_is_none : " + str(destination_obj_id_is_none))
    print("destination_obj_id_is_none length : " + str(len(destination_obj_id_is_none)))


    print("destination_narrative_obj_id_lookup length: " + str(len(destination_narrative_obj_id_lookup)))
    print("destination_results_dict length: " + str(len(destination_results_dict)))

    print("total narrative_copy_count : " + str(narrative_copy_count))
    print("--- total seconds %s seconds ---" % (time.time() - start_time))
exit()

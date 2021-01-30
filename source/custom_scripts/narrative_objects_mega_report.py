from pymongo import MongoClient
from pymongo import ReadPreference
from biokbase.service.Client import Client as ServiceClient
import json as _json
import os
import mysql.connector as mysql
import requests
import time
import math
from datetime import date
from datetime import datetime

#import pprint
#pp = pprint.PrettyPrinter(indent=4)

requests.packages.urllib3.disable_warnings()

to_workspace = os.environ["WRK_SUFFIX"]

def get_narrative_and_owners(db_connection):
    """
    returns dict of keys: narrative_id and values: owner's username 
    """
    narrative_owner_dict = dict()
    cursor = db_connection.cursor()
    select_ws_owners_query =  (
        "select ws_id, username "
        "from metrics_reporting.workspaces_current "
        "where narrative_version > 0"
    )
    cursor.execute(select_ws_owners_query)
    for (ws_id, username) in cursor:
        narrative_owner_dict[ws_id] = username                             
    return narrative_owner_dict;

def get_kbase_staff(db_connection):
    """
    get set of usernames that are kbase_staf
    """
    kbase_staff_set = set()
    cursor = db_connection.cursor()
    select_staff_query =  (
        "select username from metrics.user_info "
        "where kb_internal_user = 1"
    )
    cursor.execute(select_staff_query)
    for (username) in cursor:
        kbase_staff_set.add(username[0])
    return kbase_staff_set;

def get_top_lvl_objects(db, narrative_id):
    """
    returns dict of objnumber => {"numver":#,"del":1,"hide":1}
    """
    top_level_lookup_dict = dict()

    tl_ws_obj_cursor = db.workspaceObjects.find(
        {"ws": narrative_id}, {"id": 1, "numver": 1, "del": 1, "hide": 1, "_id": 0}
    )
    for tl_object in tl_ws_obj_cursor:
        top_level_lookup_dict[tl_object["id"]] = {
            "numver": tl_object["numver"],
            "del": tl_object["del"],
            "hide": tl_object["hide"],
        }
    return top_level_lookup_dict;    

def process_narrative_objects(db, narrative_id, top_lvl_object_lookup, kbase_staff_set, owner_username):    
    """
    goes through all the workspaces objects for a narrative gets data from Mongo and also the provenance
    prints out: 
    Object_ID
    Narrative_ID
    Version
    Owner_Username
    KBase_Staff
    Data_Type
    Core_Data_Type
    Size
    Creation_Date
    Created_By
    Created_By_KBase_staff
    Is_Top_Lvl
    Is_deleted
    Is_hidden
    Copied
    Created_By_Method
    Input_object_ids
    """
    ws_objects_dict = dict()
    #key is full reference 12/2/3   ws_id / obj_id / ver_num
    #to a second level dict the other keys and values.
    provenance_obj_refs = set()
    provenance_param_dict = dict()
    provenance_is_deleted_dict = dict()

    provenance_id_obj_ref_dict = dict()
    
    ws_obj_vers_cursor = db.workspaceObjVersions.find(
            {"ws": narrative_id},
            {
                "id":1,
                "ver":1,
                "type": 1,
                "savedate":1,
                "savedby":1,
                "size":1,
                "copied":1,
                "provenance":1
            },
	)    
    for ws_obj_ver in ws_obj_vers_cursor:
        object_type_full = ws_obj_ver["type"]
        (core_object_type, object_spec_version) = object_type_full.split("-")
        obj_ref = str(narrative_id) + "/" + str(ws_obj_ver["id"]) + "/" + str(ws_obj_ver["ver"])

        #do top lvl object logic here (remember that lower level objects inherit is_deleted and is_hidden
        is_top_lvl = 0
        if ws_obj_ver["ver"] == top_lvl_object_lookup[ws_obj_ver["id"]]["numver"]:
            is_top_lvl = 1
        is_hidden = top_lvl_object_lookup[ws_obj_ver["id"]]["hide"]
        is_deleted = top_lvl_object_lookup[ws_obj_ver["id"]]["del"]

        #KBase_staff_checks
        owner_kbase_staff = 0
        if owner_username in kbase_staff_set:
            owner_kbase_staff = 1
        created_by_kbase_staff = 0            
        if ws_obj_ver["savedby"] in kbase_staff_set:
            created_by_kbase_staff = 1

        if ws_obj_ver["provenance"]:
            provenance_id_obj_ref_dict[ws_obj_ver["provenance"]] = obj_ref 
        #BUILD UP THE OBJECTS        
        ws_objects_dict[obj_ref] = {
            "Object_ID" : ws_obj_ver["id"],
            "Narrative_ID" : narrative_id,
            "Version" : ws_obj_ver["ver"],
            "Owner_Username" : owner_username,
            "Owner_KBase_Staff" : owner_kbase_staff,
            "Data_Type" : object_type_full,
            "Core_Data_Type" : core_object_type,
            "Size" : ws_obj_ver["size"],
            "Creation_Date" : ws_obj_ver["savedate"],
            "Created_By" : ws_obj_ver["savedby"],
            "Created_By_KBase_Staff" : created_by_kbase_staff,
            "Copied" : ws_obj_ver["copied"],
            "Is_Top_Lvl" : is_top_lvl,
            "Is_deleted" : is_deleted,
            "Is_hidden" : is_hidden,
            "Created_By_Method" : None,
            "Input_object_ids" : None            
            }
#        print(str(obj_ref) + " : " + str(is_deleted) )

    temp_ws_objects_dict = get_provenamce_info(db, provenance_id_obj_ref_dict)
    for obj_ref in temp_ws_objects_dict:
        ws_objects_dict[obj_ref].update(temp_ws_objects_dict[obj_ref])

    # PRINT OUT THE OBJECT LINES
    for ws_obj_ref in ws_objects_dict:
        print("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" %
              (
                  ws_objects_dict[ws_obj_ref]["Object_ID"],
                  ws_objects_dict[ws_obj_ref]["Narrative_ID"],
                  ws_objects_dict[ws_obj_ref]["Version"],
                  ws_objects_dict[ws_obj_ref]["Owner_Username"],
                  ws_objects_dict[ws_obj_ref]["Owner_KBase_Staff"],
                  ws_objects_dict[ws_obj_ref]["Data_Type"],
                  ws_objects_dict[ws_obj_ref]["Core_Data_Type"],
                  ws_objects_dict[ws_obj_ref]["Size"],
                  ws_objects_dict[ws_obj_ref]["Creation_Date"],
                  ws_objects_dict[ws_obj_ref]["Created_By"],
                  ws_objects_dict[ws_obj_ref]["Created_By_KBase_Staff"],
                  ws_objects_dict[ws_obj_ref]["Is_Top_Lvl"],
                  ws_objects_dict[ws_obj_ref]["Is_deleted"],
                  ws_objects_dict[ws_obj_ref]["Is_hidden"],
                  ws_objects_dict[ws_obj_ref]["Copied"],
                  ws_objects_dict[ws_obj_ref]["Created_By_Method"],
                  ws_objects_dict[ws_obj_ref]["Input_object_ids"]
              )
        )
    return 1;

def get_provenamce_info(db, provenance_id_obj_ref_dict):
    return_dict = dict()
    provenance_ids_list = list(provenance_id_obj_ref_dict.keys())
    iterations = math.ceil(len(provenance_ids_list)/1000)
    i = 0
    
    while i < iterations:
        # Loop through the objects do up to 1000 at a time
        sub_list_provenance_ids = list()
        if i < iterations:
            index_start = i * 1000
            index_end = ((i + 1) * 1000) - 1
            if i == (iterations - 1):
                # do up to end of the list
                index_end = -1
            sub_list_provenance_ids = provenance_ids_list[index_start:index_end]
            
            # Get the provenance information
            prov_cursor = db.provenance.find({"_id" : { "$in": provenance_ids_list}},{"_id" : 1, "actions" : 1})
            for prov in prov_cursor:
                all_method_version_list = list()
                all_input_objects_list = list()
                for action in prov["actions"]:
                    service = ""
                    method = ""
                    # Total Methods list
                    if "service" in action:
                        service = str(action["service"])
                    if "method" in action:
                        method = str(action["method"])
                    # Total input objects list
                    if "wsobjs" in action:
                        input_obj_list = action["wsobjs"]
                    all_method_version_list.append(service + "/" + method)
                    temp_inputs = "[" + ",".join(input_obj_list) + "]"
                    all_input_objects_list.append(temp_inputs)
                return_dict[provenance_id_obj_ref_dict[prov["_id"]]] = dict()
                return_dict[provenance_id_obj_ref_dict[prov["_id"]]]["Created_By_Method"] = "[" + ",".join(all_method_version_list) + "]"
                return_dict[provenance_id_obj_ref_dict[prov["_id"]]]["Input_object_ids"] = "[" + ",".join(all_input_objects_list) + "]"
        i+=1
    return return_dict

def narrative_objects_main():
    """
    Is the "main" function to get the object data for all the workspace objects.
    The goal is to print out the following columns for each workspace object (if possible)
    Object_ID
    Narrative_ID
    Version
    Owner_Username
    Owner_KBase_Staff
    Data_Type
    Core_Data_Type
    Size
    Creation_Date
    Created_By
    Created_By_KBase_staff
    Is_Top_Lvl
    Is_deleted
    Is_hidden
    Copied
#    Created_By_Method
#    Input_object_ids
    """
    start_time = time.time()

    metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
    mongoDB_metrics_connection = os.environ["MONGO_PATH"]
    to_workspace = os.environ["WRK_SUFFIX"]
    
    client = MongoClient(mongoDB_metrics_connection + to_workspace)
    db = client.workspace

    sql_host = os.environ["SQL_HOST"]
    query_on = os.environ["QUERY_ON"]

    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    narrative_owners_lookup = get_narrative_and_owners(db_connection)
    kbase_staff_set = get_kbase_staff(db_connection)
#    print(str(narrative_owners_lookup))

#    print("Pre removal narrative count : " + str(len(narrative_owners_lookup)));
# TO DO A SUBSET
#    temp_narrative_owners_lookup = dict()
#    for narrative_id in narrative_owners_lookup:
#        if narrative_id == 78503:  
#        if narrative_id == 79132:  
#        if narrative_id >= 79132:  #1178
#        if narrative_id >= 80232:  #142
#        if narrative_id >= 80247 and narrative_id <= 80254:
#        if narrative_id >= 80249 and narrative_id <= 80252:
#            temp_narrative_owners_lookup[narrative_id] = narrative_owners_lookup[narrative_id]
#    narrative_owners_lookup = temp_narrative_owners_lookup
#    print("Post removal narrative count : " + str(len(narrative_owners_lookup)));
#    return 1;

#    print(str(kbase_staff_set))
    db_connection.close()

    #print column headers
    print(
        "Object_ID\tNarrative_ID\tVersion\tOwner_Username\tOwner_KBase_Staff\tData_Type\tCore_Data_Type\t",
        "Size\tCreation_Date\tCreated_By\tCreated_By_KBase_Staff\tIs_Top_Lvl\tIs_deleted\tIs_hidden\tCopied\t",
        "Created_By_Method\tInput_object_ids"
        )

    # connect to workspace
    mongoDB_metrics_connection = os.environ["MONGO_PATH"]
    to_workspace = os.environ["WRK_SUFFIX"]
    client = MongoClient(mongoDB_metrics_connection + to_workspace)
    db = client.workspace
    
    for narrative_id in sorted(narrative_owners_lookup):
        #top_lvl_object_lookup = dict: key obj_id , version version_number 
        top_lvl_object_lookup = get_top_lvl_objects(db, narrative_id)
#        print(str(top_lvl_object_lookup))
        process_narrative_objects(db, narrative_id, top_lvl_object_lookup,
                                  kbase_staff_set,
                                  narrative_owners_lookup[narrative_id])
    
    total_time = time.time() - start_time

#    print("--- total time  %s seconds ---" % (total_time))
    return 1;


narrative_objects_main()

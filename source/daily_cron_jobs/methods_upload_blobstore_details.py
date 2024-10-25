from pymongo import MongoClient
from pymongo import ReadPreference
#from biokbase.workspace.client import Workspace
#from installed_clients.AbstractHandleClient import AbstractHandle as HandleService
from biokbase.service.Client import Client as ServiceClient
import json as _json
import os
import mysql.connector as mysql
import methods_upload_user_stats
import requests
#import time
#from splitting import split_sequence
#from datetime import date
#from datetime
import datetime, time

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

yesterday = datetime.date.today() - datetime.timedelta(days=1)
start_time = time.time()

#################
#
#    Creates lookup for size by blobstore_id
#
################
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

###################
#
#    Create a lookup for blobstore_id by handle_id
#
###################
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

####################
#
#     GETS EXISTING BLOBSTORE RECORDS to see if new insert needs to be done
#
###################
def get_existing_blobstore_details_records (db_connection):
    existing_bs_details_cursor = db_connection.cursor(buffered=True)
    existing_bs_details_statement = ("select blobstore_id, ws_obj_id, core_ws_obj_id, is_deleted from blobstore_detail")
    existing_bs_details_cursor.execute(existing_bs_details_statement)
    existing_records_set = set()
    existing_deleted_blobstore_details_set = set()
    for (blobstore_id, ws_obj_id, core_ws_obj_id, is_deleted) in existing_bs_details_cursor:
        lookup_key = blobstore_id + "::" + ws_obj_id
        existing_records_set.add(lookup_key)
        if is_deleted == 1:
            existing_deleted_blobstore_details_set.add(core_ws_obj_id)
    return (existing_records_set, existing_deleted_blobstore_details_set)

#################
#
#    Lookup for the first save date for each blobstore id
#
################
def get_existing_bsid_first_save_date (db_connection):
    bsid_first_save_date_cursor = db_connection.cursor(buffered=True)
    bsid_first_save_date_statement = ("select blobstore_id, min(save_date) as first_save_date from blobstore_detail group by blobstore_id")
    bsid_first_save_date_cursor.execute(bsid_first_save_date_statement)
    bsid_first_save_date_dict = {}
    for (blobstore_id, first_save_date) in bsid_first_save_date_cursor:
        bsid_first_save_date_dict[blobstore_id] = first_save_date
    return bsid_first_save_date_dict

################
#
#    Populates user_info table, this gets triggered when an user is not in the user_info table.
#    This insures the foreign key does not fail.
#
################
def populate_user_info_table():
    print("Blobstore refreshing of User Stats Upload (UTC)")
    user_stats_dict = methods_upload_user_stats.get_user_info_from_auth2()
    user_stats_dict = methods_upload_user_stats.get_internal_users(user_stats_dict)
    user_stats_dict = methods_upload_user_stats.get_user_orgs_count(user_stats_dict)
    user_stats_dict = methods_upload_user_stats.get_user_narrative_stats(user_stats_dict)
    #user_stats_dict = methods_upload_user_stats.get_institution_and_country(user_stats_dict)
    user_stats_dict = methods_upload_user_stats.get_profile_info(user_stats_dict)
    print("--- gather data %s seconds ---" % (time.time() - start_time))
    methods_upload_user_stats.upload_user_data(user_stats_dict)
    print("Refresh of Upload user stats completed")

##############
#
#    Creates set of usernames in user_info. this is used to make sure the username that is seen in the
#    wsObjVersion is already in user_info table
#
#############
def get_usernames (db_connection):
    usernames_cursor = db_connection.cursor(buffered=True)
    usernames_statement = ("select username, user_id from metrics.user_info")
    usernames_cursor.execute(usernames_statement)
    temp_usernames_set = set()
    for (username, user_id) in usernames_cursor:
        temp_usernames_set.add(username)
    print("Usernames length : " + str(len(temp_usernames_set)))
    return temp_usernames_set

#############
#
#  creates set of deleted objects in the workspace collection
#
############
#def get_deleted_workspace_objects_set():
#    deleted_objects = set()
#    ws_obj_deleted_cursor = db.workspaceObjects.find({"del":True},{"_id":0, "ws": 1,"id":1})
#    for ws_obj_deleted in ws_obj_deleted_cursor:
#        deleted_temp_ws_id = ws_obj_deleted["ws"]
#        deleted_obj_id = ws_obj_deleted["id"]
#        deleted_ws_obj_id = str(deleted_temp_ws_id) + "/" + str(deleted_obj_id)
#        deleted_objects.add(deleted_ws_obj_id)
#    return deleted_objects

##############
#
#   creates set of ws_obj_ids that also have a handle
#
#############
def get_deleted_workspace_objects_set():
    deleted_workspace_objects_set = set()
    ws_obj_deleted_cursor = db.workspaceObjects.find({"del":True},{"_id":0, "ws": 1,"id":1})
    for ws_obj_deleted in ws_obj_deleted_cursor:
        deleted_temp_ws_id = ws_obj_deleted["ws"]
        deleted_obj_id = ws_obj_deleted["id"]
        deleted_ws_obj_id = str(deleted_temp_ws_id) + "/" + str(deleted_obj_id)
        deleted_workspace_objects_set.add(deleted_ws_obj_id)

    deleted_objects_with_handles_set = set()
    ws_obj_vers_cursor = db.workspaceObjVersions.find(
        {
            "extids.handle" : { "$exists": True },
        },
        {
            "ws": 1,
            "id": 1,
            "_id": 0,
        },
        no_cursor_timeout=True
    )
    for ws_obj_ver in ws_obj_vers_cursor:
        object_id = str(ws_obj_ver["ws"]) + "/" + str(ws_obj_ver["id"])
        if object_id in deleted_workspace_objects_set:
            deleted_objects_with_handles_set.add(object_id)    
    return (deleted_workspace_objects_set,deleted_objects_with_handles_set)

#############
#
#  creates set of deleted objects already in the blobstore_detail MySQL table
#
############
def get_existing_blobstore_detail_ws_objects (db_connection):
    deleted_ws_obj_cursor = db_connection.cursor(buffered=True)
    deleted_ws_obj_statement = ("select core_ws_obj_id, is_deleted from metrics.blobstore_detail where is_deleted = 1")
    deleted_ws_obj_cursor.execute(deleted_ws_obj_statement)
    existing_bs_deleted_objects_set = set()
    for (core_ws_obj_id, is_deleted) in deleted_ws_obj__cursor:
        existing_bs_deleted_objects_set.add(core_ws_obj_id)
    print("Existing Blobstore deleted ws_obj set length : " + str(len(existing_bs_deleted_objects_set)))
    return existing_bs_deleted_objects_set



############
#
#   Gets all the blobsgtore information and uploads it into the blobstre_details table
#     Defaults to previous full if start_date and end_date is passed
#     Allows for backfilling records if specific dates are chosen
#     Note this contains logic to insure all users are user_info
#     It will duplicate existing records (so it is safe to use a datge range previously done
#     It will always figure out what was the original saver object for a blobstore based on the records present
#       in the upload and existing records in the blobstore details table
############
def upload_blobstore_details_data(start_date, end_date):
    """
    Upload blobstore_date
    """
    # object_id -> {handle=>handle, node=node, type=object_type, savedate=> sd}
    objects_with_problem_nodes_with_no_size = dict()
    objects_with_problem_handles_with_no_nodes = dict()

    running_size_total = 0

    deleted_object_with_data_found_count = 0
    deleted_object_without_data_found_count = 0
    deleted_object_without_data_found_set = set()
    
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

#    #for ws_id in ws_ids:
#    deleted_objects = set()
#    ws_obj_deleted_cursor = db.workspaceObjects.find({"del":True},{"_id":0, "ws": 1,"id":1})
#    for ws_obj_deleted in ws_obj_deleted_cursor:
#        deleted_temp_ws_id = ws_obj_deleted["ws"]
#        deleted_obj_id = ws_obj_deleted["id"]
#        deleted_ws_obj_id = str(deleted_temp_ws_id) + "/" + str(deleted_obj_id)
#        deleted_objects.add(deleted_ws_obj_id)
        
#    deleted_workspace_objects = get_deleted_workspace_objects_set()
    (deleted_workspace_objects, deleted_objects_with_handles_set) = get_deleted_workspace_objects_set()

    print("TOTAL DELETED OBJECT LENGTH: " + str(len(deleted_workspace_objects)))
    print("TOTAL DELETED OBJECT LENGTH: " + str(len(deleted_objects_with_handles_set)))
    print("--- total time for the deleted objects lookup  %s seconds ---" % (time.time() - start_time))

    ws_obj_vers_cursor = db.workspaceObjVersions.find(
        {#"ws":312,
            "extids.handle" : { "$exists": True },
            "savedate": {"$gt": start_date, "$lt": end_date},
        },
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
        if temp_ws_obj_id in deleted_workspace_objects:
            deleted_ext_ids_counter += 1
            is_deleted = 1
    #        continue
        obj_ver = ws_obj_ver["ver"]
        obj_save_date = ws_obj_ver["savedate"]
        savedby = ws_obj_ver["savedby"]
        extids = ws_obj_ver["extids"]
        handles = extids["handle"]    
#       for handle in handles:
#           handles_set.add(handle)
#           obj_copied = 0
        full_obj_id = str(ws_id) + "/" + str(obj_id) + "/" + str(obj_ver)
#        print("Full obj id : " + full_obj_id)
#        print("Object Type : " + object_type_full)
#            if (object_type != "KBaseNarrative.Narrative" and object_type != "KBaseReport.Report"):
#            if (object_type == "KBaseNarrative.Narrative" or object_type == "KBaseReport.Report"):

        ws_obj_info[full_obj_id] = {"save_date" : obj_save_date,
                                    "savedby" : savedby,
                                    "obj_type" : object_type_full,
                                    "handles" : handles,
                                    "is_deleted" : is_deleted}

    print("--- total time for the ws_object_version objects query  %s seconds ---" % (time.time() - start_time))
    
    ##########################################################################
    print("BLOBSTORE LOOKUP:")
    blobstore_lookup = make_blobstore_lookup()
#    test_counter = 0
#    for temp_key in blobstore_lookup:
#        if test_counter < 10:
#            print("ID: " + str(temp_key) + "   :::   size: " + str(blobstore_lookup[temp_key]))
#        else:
#            break
#        test_counter = test_counter + 1
    print("Total BLOBSTORE Lookuplength: " + str(len(blobstore_lookup)))

    print("--- total time for the blobstore size lookup creation  %s seconds ---" % (time.time() - start_time))

    handle_id_lookup = make_handle_id_lookup()
#    test_counter = 0
#    for temp_key in handle_id_lookup:
#        if test_counter < 10:
#            print("ID: " + str(temp_key) + "   :::   blobstore_id: " + str(handle_id_lookup[temp_key]))
#        else:
#            break
#        test_counter = test_counter + 1
    print("Total HANDLE ID lookup length: " + str(len(handle_id_lookup)))

    print("--- total time for the blobstore size lookup creation  %s seconds ---" % (time.time() - start_time))
##############################################    

    for full_obj_id in  ws_obj_info:
#        print("ws_obj_info[full_obj_id][handles] : " + str(ws_obj_info[full_obj_id]["handles"]))
        for handle in ws_obj_info[full_obj_id]["handles"]:
            blobstore_id = None
            (kbh_prefix, str_handle_id) = handle.split("_")
            if int(str_handle_id) in handle_id_lookup:
                blobstore_id = handle_id_lookup[int(str_handle_id)]
            else:
                objects_with_problem_handles_with_no_nodes[full_obj_id] = ws_obj_info[full_obj_id]
                if ws_obj_info[full_obj_id]["is_deleted"] == 1:
                    deleted_object_without_data_found_count += 1
                    (temp_core_object_id, temp_ver) = full_obj_id.rsplit("/",1) 
                    deleted_object_without_data_found_set.add(temp_core_object_id)

            if blobstore_id and blobstore_id in blobstore_lookup:
                if blobstore_id not in blobstore_object_results:
                    blobstore_object_results[blobstore_id] = dict()
                blobstore_object_results[blobstore_id][full_obj_id] = (ws_obj_info[full_obj_id]["save_date"],
                                                                       ws_obj_info[full_obj_id]["savedby"])
#                print("Blobstore lookup file_size : " + str(blobstore_lookup[blobstore_id]))
                if ws_obj_info[full_obj_id]["is_deleted"] == 1:
                    deleted_object_with_data_found_count += 1
                file_size = blobstore_lookup[blobstore_id]
                running_size_total = running_size_total + file_size
            else:
#               print("HUGE PROBLEM: obj_id : " + full_obj_id +  " blobstore_id: "  + str(blobstore_id) + " IS NOT IN THE LOOKUP")
#               del blobstore_object_results[blobstore_id]
                 objects_with_problem_nodes_with_no_size[full_obj_id] = ws_obj_info[full_obj_id]
                 if ws_obj_info[full_obj_id]["is_deleted"] == 1:
                     deleted_object_without_data_found_count += 1
                     (temp_core_object_id, temp_ver) = full_obj_id.rsplit("/",1)
                     deleted_object_without_data_found_set.add(temp_core_object_id)

    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)                
#    update_zero_orig_saver_cursor = db_connection.cursor(prepared=True)
#    blobstore_detail_zero_orig_saver_update_statement = (
#        "update metrics.blobstore_detail "
#        "set orig_saver = 0  where blobstore_id = %s;"
#    )

#    update_cursor = db_connection.cursor(prepared=True)
#    blobstore_detail_update_statement = (
#        "update metrics.blobstore_detail "
#        "set orig_saver = 1  where blobstore_id = %s and ws_obj_id = %s;"
#    )
                
    bsid_first_save_date_dict = get_existing_bsid_first_save_date(db_connection)
    existing_blobstore_records, existing_deleted_blobstore_details_set = get_existing_blobstore_details_records(db_connection)
    usernames_set = get_usernames(db_connection)
    print("Usernames length = " + str(len(usernames_set)))
    db_connection.close()

    insert_count = 0
    needed_existing_update_orig_saver_count = 0
    skip_insert_because_exists_count = 0

#    loop over all the blobstore details and pull together all the needed information and do the inserts
    for blobstore_id in blobstore_object_results:
        db_connection = mysql.connect(
            host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
        )
        cursor = db_connection.cursor()
        query = "use " + query_on
        cursor.execute(query)
        bsid_new_first_save_date = None
        bsid_new_first_save_date_ws_obj_id = None    
        existing_bsid_first_save_date = None
        insert_cursor = db_connection.cursor(prepared=True)
        blobstore_detail_insert_statement = (
            "insert into metrics.blobstore_detail "
            "(blobstore_id, ws_obj_id, save_date, ws_id, size, saver_username, orig_saver, object_type, core_ws_obj_id) "
            "values(%s, %s, %s, %s, %s, %s, 0, %s, %s)"
        )

        update_zero_orig_saver_cursor = db_connection.cursor(prepared=True)
        blobstore_detail_zero_orig_saver_update_statement = (
            "update metrics.blobstore_detail "
            "set orig_saver = 0  where blobstore_id = %s;"
        )

        update_cursor = db_connection.cursor(prepared=True)
        blobstore_detail_update_statement = (
            "update metrics.blobstore_detail "
            "set orig_saver = 1  where blobstore_id = %s and ws_obj_id = %s;"
        )

        had_a_reference_ws = 0
        if blobstore_id in bsid_first_save_date_dict:
            existing_bsid_first_save_date = bsid_first_save_date_dict[blobstore_id]
        for full_ws_obj_id in blobstore_object_results[blobstore_id]:
            (ws_id, obj_id, version_number) = full_ws_obj_id.split("/")
            save_date = blobstore_object_results[blobstore_id][full_ws_obj_id][0]
            saver = blobstore_object_results[blobstore_id][full_ws_obj_id][1]
        
            lookup_key = blobstore_id + "::" + full_ws_obj_id
            if lookup_key in existing_blobstore_records:
                skip_insert_because_exists_count += 1
                continue

        #    TO GET ONLY REFERENCE GENOME WORKSPACES
#            if int(ws_id) in (19217, 16026,  28129, 80490):
#                had_a_reference_ws = 1
        #    DO INSERT SET ORIG_SAVER = 0

            if saver not in usernames_set:
                print("Usernames pre length = " + str(len(usernames_set)))
                populate_user_info_table()
                usernames_set = get_usernames(db_connection)
                print("Usernames post length = " + str(len(usernames_set)))
        
            size = blobstore_lookup[blobstore_id]
            object_type = ws_obj_info[full_ws_obj_id]["obj_type"]
            temp = full_ws_obj_id.split("/")
            core_ws_obj_id = "/".join(temp[:-1])

            input_vals = (
                blobstore_id,
                full_ws_obj_id,
                save_date,
                ws_id,
                size,
                saver,
                object_type,
                core_ws_obj_id,
            )
            insert_cursor.execute(blobstore_detail_insert_statement, input_vals)
            insert_count += 1
        
            # record is fresh and needs to be inserted.
            #DO SAVE DATE LOGIC LOOKING FOR MIN_DATE
            if (existing_bsid_first_save_date and save_date < existing_bsid_first_save_date):
                bsid_new_first_save_date = save_date
                bsid_new_first_save_date_ws_obj_id = full_ws_obj_id
            if existing_bsid_first_save_date is None:
                if (bsid_new_first_save_date is None or save_date < bsid_new_first_save_date):
                    bsid_new_first_save_date = save_date
                    bsid_new_first_save_date_ws_obj_id = full_ws_obj_id
        

#        if had_a_reference_ws == 1:
    #    AFTER ALL THE INSERTS DONE (update the record that is now the min_date, potentially change min_date from an existing or-ig_saver
        if existing_bsid_first_save_date is not None and bsid_new_first_save_date is not None:
            #meand a new seen record has lower save date than an existing one.  Should not occur.
            update_vals = (blobstore_id,)
            update_zero_orig_saver_cursor.execute(blobstore_detail_zero_orig_saver_update_statement, update_vals)
            needed_existing_update_orig_saver_count += 1
        if bsid_new_first_save_date_ws_obj_id is not None:
            update_cursor = db_connection.cursor(prepared=True)
            blobstore_detail_update_statement = (
                "update metrics.blobstore_detail "
                "set orig_saver = 1  where blobstore_id = %s and ws_obj_id = %s;"
            )
            update_vals = (blobstore_id, bsid_new_first_save_date_ws_obj_id)
            update_cursor.execute(blobstore_detail_update_statement, update_vals)
        insert_cursor.close()
        db_connection.commit()
        db_connection.close()

    # RESOLVE THE MISSING DELETED OBJECTS
    deleted_objects_to_update_set = deleted_objects_with_handles_set.difference(existing_deleted_blobstore_details_set)
    if len(deleted_objects_to_update_set) > 0:
        print("Length of core obj ids that need to be marked as deleted : " + str(len(deleted_objects_to_update_set)))
        print("length of deleted_object_without_data_found_set : " + str(len(deleted_object_without_data_found_set)))
        db_connection = mysql.connect(
            host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
        )
        cursor = db_connection.cursor()
        query = "use " + query_on
        cursor.execute(query)

        update_deleted_objects_cursor = db_connection.cursor()
        update_deleted_objects_statement = ("update metrics.blobstore_detail set is_deleted = 1 where core_ws_obj_id = %s;")
        for core_deleted_obj_id in deleted_objects_to_update_set:
            update_deleted_objects_vals = (core_deleted_obj_id,)
            update_deleted_objects_cursor.execute(update_deleted_objects_statement, update_deleted_objects_vals)
        update_deleted_objects_cursor.close
        db_connection.commit()
        db_connection.close()

    # UNDELETE THE OBJECTS THAT HAVE BEEN UNDELETED
    undeleted_objects_to_update_set = existing_deleted_blobstore_details_set.difference(deleted_objects_with_handles_set)
    if len(undeleted_objects_to_update_set) > 0:
        print("Length of core obj ids that need to be marked as undeleted : " + str(len(undeleted_objects_to_update_set)))
        db_connection = mysql.connect(
            host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
        )
        cursor = db_connection.cursor()
        query = "use " + query_on
        cursor.execute(query)

        update_undeleted_objects_cursor = db_connection.cursor()
        update_undeleted_objects_statement = ("update metrics.blobstore_detail set is_deleted = 0 where core_ws_obj_id = %s;")
        for core_undeleted_obj_id in undeleted_objects_to_update_set:
            update_undeleted_objects_vals = (core_undeleted_obj_id,)
            update_undeleted_objects_cursor.execute(update_undeleted_objects_statement, update_undeleted_objects_vals)
        update_undeleted_objects_cursor.close
        db_connection.commit()
        db_connection.close()
        
        
    #print("objects_with_problem_nodes_with_no_size : " + str(objects_with_problem_nodes_with_no_size))            
    print("TOTAL objects_with_problem_nodes_with_no_size : " + str(len(objects_with_problem_nodes_with_no_size)))

    #print("objects_with_problem_handles_with_no_nodes : " + str(objects_with_problem_handles_with_no_nodes))
    print("TOTAL objects_with_problem_handles_with_no_nodes : " + str(len(objects_with_problem_handles_with_no_nodes)))

    print("deleted_object_with_data_found_count :" + str(deleted_object_with_data_found_count))
    print("deleted_object_without_data_found_count :" + str(deleted_object_without_data_found_count))

#    print("blobstore_object_results :  " +  str(blobstore_object_results))
#    for blobstore_id in blobstore_object_results:
#        if len( blobstore_object_results[blobstore_id]) > 5:
#            print("blobstore ID : " + str(blobstore_id))
#            print(str(blobstore_object_results[blobstore_id]))
    print("blobstore_object_results length :  " +  str(len(blobstore_object_results)))
    print("RUNNING TOTAL SIZE : " + str(running_size_total))

    obj_id_set = set()
    for blobstore_id in blobstore_object_results :
        for obj_id in  blobstore_object_results[blobstore_id]:
            obj_id_set.add(obj_id)
    print("Total number of objects with handles that could be fully determined : " + str(len(obj_id_set)))

    print("Total ext_ids objects that were deleted : " + str(deleted_ext_ids_counter))

    #print("blobstore_object_results : " + str(blobstore_object_results))


    print("Insert Count = " + str(insert_count))
    print("needed_existing_update_orig_saver_count = " + str(needed_existing_update_orig_saver_count))
    print("skip_insert_because_exists_count = " + str(skip_insert_because_exists_count))

    print("--- total seconds %s seconds ---" % (time.time() - start_time))
    #db_connection.commit()
    #db_connection.cLOSE()

    ####################
    # END upload_blobstore_details_data
    ###################



#####################
#
#   Essentially the main caller program that deals with start and end date information
#   Whether there were passed values or the defaut of the previous full day
#
####################
def process_blobstore_details_data(
            start_date=datetime.datetime.combine(yesterday, datetime.datetime.min.time()),
            end_date=datetime.datetime.combine(yesterday, datetime.datetime.max.time()),
        ):
    # get mongo set up
    #    client_blobstore = MongoClient(mongoDB_metricsro_connection + to_blobstore)
    client_blobstore = MongoClient(mongoDB_metrics_connection + to_blobstore)
    db_blobstore = client_blobstore.blobstore

    print("############################################")
    print("START TIME (UTC): " + str(datetime.datetime.utcnow()))
    start_time = time.time()

    # From str to datetime, defaults to zero time.
    if type(start_date) == str:
        start_date_partial = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        start_date = datetime.datetime.combine(
            start_date_partial, datetime.datetime.min.time()
        )
        end_date_partial = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        end_date = datetime.datetime.combine(
            end_date_partial, datetime.datetime.max.time()
        )

    print("Start date : " + str(start_date))
    print("End date : " + str(end_date))
        
    upload_blobstore_details_data(start_date, end_date)
    print("############################################")    
#exit()

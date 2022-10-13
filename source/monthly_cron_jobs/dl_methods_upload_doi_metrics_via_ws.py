from pymongo import MongoClient
#from pymongo import ReadPreference
import os
import mysql.connector as mysql
import requests
import populate_downloading_apps

#FOR RE IF WE GO BACK TO IT
from arango import ArangoClient

from datetime import date
#from datetime import datetime
import datetime
import time

# import pprint
requests.packages.urllib3.disable_warnings()

# NOTE get_user_info_from_auth2 sets up the initial dict.
# The following functions update certain fields in the dict.
# So get_user_info_from_auth2 must be called before get_internal_users and get_user_orgs_count

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
mongoDB_metrics_connection = os.environ["MONGO_PATH"]

sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]

to_workspace = os.environ["WRK_SUFFIX"]

re_host_url = os.environ["RE_HOST_URL"]
re_username = os.environ["RE_USERNAME"]
re_pwd = os.environ["RE_PWD"]

re_client = ArangoClient(hosts=re_host_url)
re_db = re_client.db('prod', username=re_username, password=re_pwd)

#max_in_string_length = 0
#max_list_length = 0

def build_copy_lookup(db):
    #Build source to copy lookup
    #builds a dict of keys of source_object_id and values of set of copied_object_ids
    ##This is the most time consuming part

    copied_object_count = 0
    copied_to_lookup_dict = dict()
    ws_obj_vers_cursor = db.workspaceObjVersions.find( {"copied" : {"$ne": None}},{"copied":1, "ws":1, "id":1, "ver":1, "type":1,"_id":0})
    for ws_obj_ver in ws_obj_vers_cursor:
        # check if it is a narrative type
        object_type_full = ws_obj_ver["type"]
        object_type = object_type_full.split("-")[0]
#        if  object_type == "KBaseReport.Report":
            #object_type == "KBaseNarrative.Narrative" or 
#            continue
        copied_object_count += 1
        ############################
#        if "53247/" in  ws_obj_ver["copied"]:
#            print("Found 53247/ in :" + ws_obj_ver["copied"])#
#        txt = ws_obj_ver["copied"]
#        x = txt.split("/")
#        if x[0] != int(53247):
#            continue
        #############################
        full_obj_id_of_copy = str(ws_obj_ver["ws"]) + "/" + str(ws_obj_ver["id"]) + "/" + str(ws_obj_ver["ver"])
        if ws_obj_ver["copied"] not in copied_to_lookup_dict:
            copied_to_lookup_dict[ws_obj_ver["copied"]] = list()
        copied_to_lookup_dict[ws_obj_ver["copied"]].append(full_obj_id_of_copy)
#    print(str(copied_to_lookup_dict))
#    print("Total object sources copied: " + str(len(copied_to_lookup_dict)))
#    print("Total resulting copies: " + str(copied_object_count))
#    copy_count_dict = dict()
#    for copied_object in copied_to_lookup_dict:
#        temp_length = str(len(copied_to_lookup_dict[copied_object]))
#        if temp_length not in copy_count_dict:
#            copy_count_dict[temp_length] = 1
#        else:
#            copy_count_dict[temp_length] += 1
#    for key, value in sorted(copy_count_dict.items(), key=lambda item: int(item[0])):
#        print(key, value)
    return copied_to_lookup_dict

def get_workspace_owners(db):
    #builds a dict of keys of ws_id and values of usernames who own that ws.
    ws_owners_lookup = dict()
    ws_cursor = db.workspaces.find({},{"ws":1, "owner":1, "_id":0})
    for ws_item in ws_cursor:
        ws_owners_lookup[ws_item["ws"]] = ws_item["owner"]
#    print("Ws owners: " + str(ws_owners_lookup))
    return ws_owners_lookup

#def get_dois_and_narratives(cursor):
def get_dois_and_narratives():
    #creates a dict of DOIs as keys to values of a list of WS_IDS to look at.
    #If the ws_id value list is a single element the DOI is associated with the WS_ID
    #If the ws_id value list has multiple ws_ids the first ws_id is the parent organizining ws_id,
    #the remainder in the list are childredn ws_ids.

    db_connection = mysql.connect(
                host=sql_host,  # "mysql1", #"localhost",
                user="metrics",  # "root",
                passwd=metrics_mysql_password,
                database="metrics",  # "datacamp"
            )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)
    # SET TABLES : USE copy_ at start of table for testing
    doi_ws_map_table = "doi_ws_map"
    doi_metrics_table = "copy_doi_metrics"

    query = "select doi_url, ws_id, is_parent_ws from " + doi_ws_map_table;
#    query = "select doi_url, ws_id, is_parent_ws from metrics.doi_ws_map";
    cursor.execute(query)
    doi_results_map = dict()
    ws_list = list()
    for row_values in cursor:
        doi = row_values[0]
        ws_id = row_values[1]
        ws_list.append(ws_id)
        is_parent = row_values[2]
        if doi not in doi_results_map:
            doi_results_map[doi] = dict()
            doi_results_map[doi]["doi_owners"] = set()
            doi_results_map[doi]["ws_ids"] = dict()
        doi_results_map[doi]["ws_ids"][ws_id] = dict()
        doi_results_map[doi]["ws_ids"][ws_id]["is_parent"] = is_parent

    # GET CURRENT MONTH
    today = date.today()
    current_month = str(today.year) + "-" + today.strftime("%m")


    check_doi_url_dict = dict()

    # Check which DOI/WS have been run this month.
    cursor.execute("select dwp.doi_url, dm.ws_id, dm.record_date "
                   "from " + doi_ws_map_table +
                   "  dwp inner join " + doi_metrics_table +
                   " dm on dwp.ws_id = dm.ws_id "
                   "where DATE_FORMAT(record_date,'%Y-%m') = %s ",(current_month,))

##    for doi_metrics_record in check_prep_cursor:
    for doi_metrics_record in cursor:
        check_doi_url = doi_metrics_record[0]
        check_doi_ws_id = doi_metrics_record[1]
        if check_doi_url not in check_doi_url_dict:
            check_doi_url_dict[check_doi_url] = set()
        check_doi_url_dict[check_doi_url].add(check_doi_ws_id)

    for check_doi_url in check_doi_url_dict:
        is_complete = 1
        
        for staged_doi_ws in doi_results_map[check_doi_url]["ws_ids"]:
            if staged_doi_ws not in check_doi_url_dict[check_doi_url]:
                is_complete = 0
        if is_complete == 1:
            # means that doi has been rreviosuly uploaded this month and needs to be removed from staged doi_results_map
            print("DOI : " + check_doi_url + " was already uploaded this month, it is being skipped.")
            del doi_results_map[check_doi_url]
        else:
            # is partial because it is showing up in the check_doi_url dict meaning something is in the DB
            # Need to remove those records from the doi_metrics table
            check_ws_list = list(check_doi_url_dict[check_doi_url])
            print("DOI : " + check_doi_url + " was partially uploaded this month, its Workspaces " + str(check_ws_list) +
                  " are being deleted from doi_metrics for the month to start fresh uploading for the DOI")
            print("This month should redo these workspaces : " + str(check_ws_list))
            for ws_id_to_delete in check_doi_url_dict[check_doi_url]:
                cursor.execute("delete from " + doi_metrics_table + " where ws_id = %s and DATE_FORMAT(record_date,'%Y-%m') = %s",
                               (ws_id_to_delete, current_month))

    db_connection.commit()
        
    print("doi_results_map : " + str(doi_results_map))
    print("check_doi_url_dict : " + str(check_doi_url_dict))
#    exit()
    return doi_results_map

def get_downloaders_set(cursor):
    #returns a set of downloadwer apps
    query = "select downloader_app_name, 1 from metrics.downloader_apps";
    cursor.execute(query)
    downloaders_set = set()
    for row_values in cursor:
        downloaders_set.add(row_values[0])
    return downloaders_set
    
def get_doi_owners_usernames(db, doi_results_map):
    #creates a set of unique usernames associated with the DOI
    #Any copies and usernames will not be counted if they are part of that list.

    for doi in doi_results_map:
        for ws_id in doi_results_map[doi]["ws_ids"]:
            ws_perm_cursor = db.workspaceACLs.find({"id":ws_id},{"user":1, "perm":1, "_id":0})
            for ws_perm in ws_perm_cursor:
                if ws_perm["perm"] > 10:
                    doi_results_map[doi]["doi_owners"].add(ws_perm["user"])
#    print(str(doi_results_map))
    return doi_results_map

def get_objects_for_ws(db, ws_id):
    #gets a list of ws_references from the passed WS_ID
    objects_to_check_copies_list = list()
    ws_objs_cursor = db.workspaceObjVersions.find({"ws":ws_id},{"type":1, "id":1, "ver":1,"_id":0})
    for ws_obj in ws_objs_cursor:
        full_obj_type = ws_obj["type"]
        core_type = full_obj_type.split('-',1)[0]
        obj_id = str(ws_id) + "/" + str(ws_obj["id"]) + "/" + str(ws_obj["ver"])
        objects_to_check_copies_list.append(obj_id)
    return objects_to_check_copies_list

def build_doi_ws_objects_types_lookup(db, doi_workspaces):
    # gets all the types for all the doi_ws_objects
    #print("doi_workspaces : " + str(doi_workspaces))
    doi_ws_objects_type_dict = dict() # top key doi_ws => dict ( key is obj_id : value is type)
    ws_obj_vers_cursor = db.workspaceObjVersions.find({"ws":{"$in":doi_workspaces}},{"ws":1,"id":1,"ver":1,"type":1,"_id":0})
    for ws_obj_ver in ws_obj_vers_cursor:
        object_ref = str(ws_obj_ver["ws"]) + "/" + str(ws_obj_ver["id"]) + "/" + str(ws_obj_ver["ver"])
        if ws_obj_ver["ws"] not in doi_ws_objects_type_dict:
            doi_ws_objects_type_dict[ws_obj_ver["ws"]] = dict()
        doi_ws_objects_type_dict[ws_obj_ver["ws"]][object_ref] = ws_obj_ver["type"]    
    return doi_ws_objects_type_dict

def quick_parent_lookup(doi_results_map):
    #returns lookup of children WS to find the parent WS

    child_parent_ws_id_lookup = dict()
    for doi in doi_results_map:
        parent_ws_id = None
        child_ws_list = list()
        for ws_id in doi_results_map["ws_ids"]:
            if doi_results_map["ws_ids"][ws_id]["is_parent"] == 1:
                parent_ws_id =ws_id
            else:
                child_ws_list.append(ws_id)
        if parent_ws_id is None:
            # Raise an error should not be the case.  Means the WS data is incorrect
            raise ValueError("The data in doi_ws_map is not set up properly every doi must have 1 parent ws (even if no children).")
        for child_ws_id in child_ws_list:
            child_parent_ws_id_lookup[child_ws_id] = parent_ws_id
    return child_parent_ws_id_lookup

def grow_derived_dict_mongo(db, doi_ws_id, copied_to_lookup_dict, master_dict, last_iteration_dict, copy_only):
    #grows the dict of copied and transformed (used as input to make other objects)(object ids as keys => values is object type)
    # - collectively called derived objects for a WS
    #returns the master_dict and makes along the way the dict for the next iteration
    next_iteration_dict = dict()
#    return_master_dict = master_dict
    #print("Last Iteration DICT:" + str(last_iteration_dict))

#    print("master_dict  : " + str(master_dict))
#    print("last_iteration_dict 1  : " + str(last_iteration_dict))
    
    # DO COPY TREE CHECKING FIRST
    for object_ws_obj_id in last_iteration_dict:
        if object_ws_obj_id in copied_to_lookup_dict:
            copied_object_type = last_iteration_dict[object_ws_obj_id]
            for copied_to_obj_id in copied_to_lookup_dict[object_ws_obj_id]:
                temp_ws_id_string = copied_to_obj_id.split("/",1)[0]
                if int(temp_ws_id_string) != doi_ws_id:
                    next_iteration_dict[copied_to_obj_id] = copied_object_type

                    
    if copy_only == 0 :
        #useing ws_obj_versions to lookup
        #DOING ALL PREVIOUS ITERATION IN one call
#        global max_in_string_length
#        global max_list_length
#        temp_in_string_length = len(''.join(list(last_iteration_dict)))
#        if temp_in_string_length > max_in_string_length:
#            max_in_string_length = temp_in_string_length
#            max_list_length = len(list(last_iteration_dict))
        # Mongo's "in" clause has a limit of 16.6 MB. Each character is 1 byte
        # The current limit is dict for 500K which totals about 6MB with the current UPAs length
        # This gives us considerable cushion. But the 500K can be lowered if need in the future
        # But I would be surprised if UPAs get over twice the current size..

        last_iteration_length = len(last_iteration_dict)
        list_chunk = 500000
        list_chunk_counter = 0
#        print("last_iteration_dict  : " + str(last_iteration_dict))
#        print("last_iteration_dict_keys  : " + str(last_iteration_dict.keys()))
        list_of_last_iteration = list(last_iteration_dict.keys())
#        print("list_of_last_iteration : " + str(list_of_last_iteration))
        while list_chunk_counter < last_iteration_length:
            sub_last_iteration_list = list_of_last_iteration[list_chunk_counter:(list_chunk_counter + list_chunk)]
            #ws_obj_cursor = db.workspaceObjVersions.find({"provrefs":{"$in":sub_last_iteration_list}},{"ws":1,"id":1,"ver":1,"type":1, "provrefs":1,"_id":0})
            ws_obj_cursor = db.workspaceObjVersions.find({"provrefs":{"$in":sub_last_iteration_list}, "copied" : {"$eq": None}},{"ws":1,"id":1,"ver":1,"type":1, "provrefs":1,"_id":0})
            for ws_obj_item in ws_obj_cursor:
                if int(ws_obj_item["ws"]) != doi_ws_id:
                    derived_ws_obj_id = str(ws_obj_item["ws"]) + "/" + str(ws_obj_item["id"]) + "/" + str(ws_obj_item["ver"])
                    next_iteration_dict[derived_ws_obj_id] = ws_obj_item["type"]
#                    print("doi_ws_id : " + str(doi_ws_id) + "  derived_ws_obj_id : " + derived_ws_obj_id + "   IN IF")
                else:
                    derived_ws_obj_id = str(ws_obj_item["ws"]) + "/" + str(ws_obj_item["id"]) + "/" + str(ws_obj_item["ver"])
#                    print("doi_ws_id : " + str(doi_ws_id) + "  derived_ws_obj_id : " + derived_ws_obj_id + "   IN ELSE")
            list_chunk_counter += list_chunk
    if len(next_iteration_dict) > 0:
#        print("master dict pre append: " + str(master_dict))
        for next_iteration_object_key in next_iteration_dict:
#            return_master_dict[next_iteration_object_key] = next_iteration_dict[next_iteration_object_key]
            master_dict[next_iteration_object_key] = next_iteration_dict[next_iteration_object_key]
#        print("master dict post append: " + str(master_dict))
#        master_dict = grow_derived_dict_mongo(db,doi_ws_id, copied_to_lookup_dict, return_master_dict, next_iteration_dict, copy_only)
        master_dict = grow_derived_dict_mongo(db,doi_ws_id, copied_to_lookup_dict, master_dict, next_iteration_dict, copy_only)
#        print("master dict post function call: " + str(master_dict))
#    return return_master_dict
    return master_dict

        
def grow_derived_set_RE(doi_ws_id, copied_to_lookup_dict, master_set, last_iteration_set, copy_only):
    #grows the set of copied and transformed (used as input to make other objects) - collectively called derived objects for a WS
    #returns the master_set and makes along the way the set for the next iteration
    next_iteration_set = set()
    last_iteration_modified_set = set()
    #print("Last Iteration SET:" + str(last_iteration_set))
    for object_ws_obj_id in last_iteration_set:
#        print("Result of copied lookup for " + str(object_ws_obj_id) + " : " + str(copied_to_lookup_dict.get(object_ws_obj_id)))
        if object_ws_obj_id in copied_to_lookup_dict:
            for copied_to_obj_id in copied_to_lookup_dict[object_ws_obj_id]:
#                print("Parameter : " + str(doi_ws_id))
#                print("object_ws_obj_id : " + object_ws_obj_id)
#                print("Copied TO copied_to_obj_id : " + copied_to_obj_id)
                temp_ws_id_string = copied_to_obj_id.split("/",1)[0]
#                print("Copied TO temp_ws_id_string : " + str(temp_ws_id_string))
                if int(temp_ws_id_string) != doi_ws_id:
#                    print("IN IF")
#                    next_iteration_set.add(copied_to_lookup_dict[object_ws_obj_id])
                    next_iteration_set.add(copied_to_obj_id)
        if copy_only == 0 :
            # swap and replace "/" with ":" for query against RE for input objects. Prepare for RE call.
            modified_obj_id = x = object_ws_obj_id.replace("/", ":")
            last_iteration_modified_set.add(modified_obj_id)    
#    print("Next Iteration SET PRE:" + str(next_iteration_set))
    if copy_only == 0 :
        # Using RE
        cursor = re_db.aql.execute( 'LET source_list = @val '\
                                    'for id in source_list '\
                                    '  FOR doc IN ws_prov_descendant_of '\
                                    '    LET fullid =  CONCAT("ws_object_version/", id) '\
                                    '    FILTER doc._to==fullid '\
                                    '    RETURN { "input object": fullid, "resulting object": doc._from}',
                                    bind_vars={'val': list(last_iteration_modified_set)})
        next_iteration_re_outputs = set()
        results = [doc for doc in cursor]
#        print("Results: " + str(results))
        for result in results:
            output_object = result['resulting object']
            temp_output_object_id = output_object.replace("ws_object_version/","")
            final_ws_obj_id = temp_output_object_id.replace(":","/")
#            next_iteration_re_outputs.add(final_ws_obj_id)
#           print("RE RESULT: " + str(result))
#          print("INPUT OBJECT : " + result['resulting object'])
#            print("RESULTING OBJECT : " + result['resulting object'])
            temp_ws_id_string = final_ws_obj_id.split("/",1)[0]
#            print("temp_ws_id_string : " + str(temp_ws_id_string))
            if int(temp_ws_id_string) != doi_ws_id:
#                print("IN IF 2")                
                next_iteration_set.add(final_ws_obj_id)
#        print("Next Iteration SET POST:" + str(next_iteration_set))

    if len(next_iteration_set) > 0:
#        print("master set pre append: " + str(master_set))
        master_set = master_set | next_iteration_set
#        print("master set post append: " + str(master_set))
        master_set = grow_derived_set(doi_ws_id, copied_to_lookup_dict, master_set, next_iteration_set, copy_only)
#        print("master set post function call: " + str(master_set))
    return master_set



#def create_obj_to_orignal_source_object_lookup(ws_objects_to_track, copied_to_lookup_dict):
    #all_copied_objects_from_ws_list):
    #need to create 
    #copied_object_id -> source_object_id lookup
    #
#    obj_to_orignal_source_object_lookup = dict()
#    for source_ws_object in ws_objects_to_track:
#        obj_to_orignal_source_object_lookup[source_ws_object] = source_ws_object
#        new_list_of_copied_objects = list()
#        if source_ws_object in copied_to_lookup_dict:
#            new_list_of_copied_objects = copied_to_lookup_dict[source_ws_object]
#            has_another_level_of_copying = True
#            next_level_
#            for new_element_object_id in new_list_of_copied_objects:
#                obj_to_orignal_source_object_lookup[new_element_object_id] = source_ws_object
               

def determine_doi_statistics(db, doi_results_map, copied_to_lookup_dict, ws_owners_lookup, doi_ws_objects_types_lookup):
    #Populates the doi_results_map with the
    #unique set of users to ws_ids
    child_parent_ws_id_lookup = quick_parent_lookup(doi_results_map)
#    print("copied_to_lookup_dict: " + str(copied_to_lookup_dict))
    #ws_objectss_to_track = dict()
    doi_owners_usernames = doi_results_map["doi_owners"]
    total_copy_count = 0
    total_derived_count = 0
    total_copy_count_pre_filter = 0
    total_derived_count_pre_filter = 0
    total_copy_direct_pre_filter = 0
    total_derived_direct_pre_filter = 0
    for ws_id in doi_results_map["ws_ids"]:
        #print("WS ID BEING USED:" + str(ws_id))
        ws_objects_to_track_dict = dict()
        #ws_objects_to_track = doi_ws_objects_types_lookup[ws_id].keys()
        ws_objects_to_track_dict = doi_ws_objects_types_lookup[ws_id]
#        ws_objects_to_track = get_objects_for_ws(db, ws_id)
    
        #print("WS_OBJECTS_TO_TRACK: " + str(ws_objects_to_track))
        parent_ws_id = None
        if ws_id in child_parent_ws_id_lookup:
            parent_ws_id = child_parent_ws_id_lookup[ws_id]

#        print("ws_objects_to_track_dict :: " + str(ws_objects_to_track_dict))
        for ws_object_to_track in ws_objects_to_track_dict:
            ws_object_to_track_dict = {ws_object_to_track : ws_objects_to_track_dict[ws_object_to_track]}

#            print("START TRACKING : " + str(ws_object_to_track_dict))
            all_copied_only_objects_from_ws_object = grow_derived_dict_mongo(db,ws_id, copied_to_lookup_dict, ws_object_to_track_dict, ws_object_to_track_dict,1)
            #print("POST COPY ws_object_to_track_dict :: " + str(ws_object_to_track_dict))
            ws_object_to_track_dict2 = {ws_object_to_track : ws_objects_to_track_dict[ws_object_to_track]}
            #print("POST COPY ws_object_to_track_dict but reset it :: " + str(ws_object_to_track_dict))
            total_copy_direct_pre_filter += len(all_copied_only_objects_from_ws_object)
            #print("ALL COPIED ONLY _objects_from_ws_object length PRE: " + ws_object_to_track + ":::" + str(len(all_copied_only_objects_from_ws_object)))
            #print("ALL COPIED ONLY _objects_from_ws_object : " + ws_object_to_track + ":::" + str(all_copied_only_objects_from_ws_object))
            #print("all_copied_only_objects_from_ws_object : " + ws_object_to_track + ":::" + str(all_copied_only_objects_from_ws_object))
            #all_derived_objects_from_ws_object = list()
            all_derived_objects_from_ws_object = grow_derived_dict_mongo(db,ws_id, copied_to_lookup_dict, ws_object_to_track_dict2, ws_object_to_track_dict2,0)
            #print("POST DERIVED ws_object_to_track_dict :: " + str(ws_object_to_track_dict))
            total_derived_direct_pre_filter+= len(all_derived_objects_from_ws_object)
            #print("ALL DERIVED OBJECT LIST from ws object: " + str(ws_object_to_track) + " :: " + str(all_derived_objects_from_ws_object))
            #print("ALL POST COPIED ONLY _objects_from_ws_object : " + ws_object_to_track + ":::" + str(all_copied_only_objects_from_ws_object))
            #print("ALL COPIED ONLY _objects_from_ws_object length POST: " + ws_object_to_track + ":::" + str(len(all_copied_only_objects_from_ws_object)))
            #print("ALL DERIVED _objects_from_ws_object length: " + ws_object_to_track + ":::" + str(len(all_derived_objects_from_ws_object)))
#            print("END TRACKING, prefiltering : " + ws_object_to_track)
            
            for object_copied in all_copied_only_objects_from_ws_object:
                temp_copied_object_ws_id = object_copied.split("/",1)[0]
                copied_object_ws_id = int(temp_copied_object_ws_id)
#               print("copied WS : " + str( copied_object_ws_id) + "  The copied owner lookup: " + str(ws_owners_lookup[copied_object_ws_id]))
                total_copy_count_pre_filter += 1
#                if ws_owners_lookup[copied_object_ws_id] not in doi_owners_usernames:
                if copied_object_ws_id in ws_owners_lookup and ws_owners_lookup[copied_object_ws_id] not in doi_owners_usernames:
                    doi_results_map["ws_ids"][ws_id]["unique_users"].add(ws_owners_lookup[copied_object_ws_id])
                    doi_results_map["ws_ids"][ws_id]["unique_workspaces"].add(copied_object_ws_id)
                    if ws_object_to_track not in doi_results_map["ws_ids"][ws_id]["copied_only_objects"]:
                        doi_results_map["ws_ids"][ws_id]["copied_only_objects"][ws_object_to_track] = dict()
                    doi_results_map["ws_ids"][ws_id]["copied_only_objects"][ws_object_to_track][object_copied] = all_copied_only_objects_from_ws_object[object_copied]
                    total_copy_count += 1

                    if ws_id in child_parent_ws_id_lookup:
                        parent_ws_id = child_parent_ws_id_lookup[ws_id]
                        doi_results_map["ws_ids"][parent_ws_id]["unique_users"].add(ws_owners_lookup[copied_object_ws_id])
                        doi_results_map["ws_ids"][parent_ws_id]["unique_workspaces"].add(copied_object_ws_id)
#            print("END copy only filtering : " + ws_object_to_track)
########################
#SEEMS LIKE THIS LINE SHOULD BE 1 tab lest.
########################
            for object_derived in all_derived_objects_from_ws_object:
                temp_derived_object_ws_id = object_derived.split("/",1)[0]
                derived_object_ws_id = int(temp_derived_object_ws_id)
                total_derived_count_pre_filter += 1
#               print("copied WS : " + str( copied_object_ws_id) + "  The copied owner lookup: " + str(ws_owners_lookup[copied_object_ws_id]))
                if object_derived not in all_copied_only_objects_from_ws_object:
 #                       print("object_derived : " + str(object_derived))
 #                       print("object_derived NON STRING: " + object_derived) 
                        #means the obect used as input at one point
                        #ndeded to add derived_object_ws_id in ws_owners_lookup because brand new workspaces may not be part of the lookup.
                    if derived_object_ws_id in ws_owners_lookup and ws_owners_lookup[derived_object_ws_id] not in doi_owners_usernames:
                        doi_results_map["ws_ids"][ws_id]["unique_users"].add(ws_owners_lookup[derived_object_ws_id])
                        doi_results_map["ws_ids"][ws_id]["unique_workspaces"].add(derived_object_ws_id)
                        if ws_object_to_track not in doi_results_map["ws_ids"][ws_id]["derived_objects"]:
#                                doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"][ws_object_to_track] = set()
                            doi_results_map["ws_ids"][ws_id]["derived_objects"][ws_object_to_track] = dict()
#                            doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"][ws_object_to_track].add(object_derived)
                        doi_results_map["ws_ids"][ws_id]["derived_objects"][ws_object_to_track][object_derived] = all_derived_objects_from_ws_object[object_derived]
                        total_derived_count += 1
                            
                        if ws_id in child_parent_ws_id_lookup:
                            parent_ws_id = child_parent_ws_id_lookup[ws_id]
                            doi_results_map["ws_ids"][parent_ws_id]["unique_users"].add(ws_owners_lookup[derived_object_ws_id])
                            doi_results_map["ws_ids"][parent_ws_id]["unique_workspaces"].add(derived_object_ws_id)
#                                if ws_object_to_track not in doi_results_map[doi]["ws_ids"][parent_ws_id]["derived_objects"]:
#                                    doi_results_map[doi]["ws_ids"][parent_ws_id]["derived_objects"][ws_object_to_track] = set()
#                                    doi_results_map[doi]["ws_ids"][parent_ws_id]["derived_objects"][ws_object_to_track] = dict()
#                                doi_results_map[doi]["ws_ids"][parent_ws_id]["derived_objects"][ws_object_to_track].add(object_derived)
#                                doi_results_map[doi]["ws_ids"][parent_ws_id]["derived_objects"][ws_object_to_track][object_derived] = all_derived_objects_from_ws_object[object_derived]

#            print("END derived filtering : " + ws_object_to_track)
###############3
# END OF UNINDENTED BY 1
###############3


            # code to check if two data structres are the same:
#            if ws_object_to_track in  doi_results_map["ws_ids"][ws_id]["derived_objects"]:
#                for object_to_test in doi_results_map["ws_ids"][ws_id]["derived_objects"][ws_object_to_track]:
#                        print("ws_object_to_track : " + ws_object_to_track)
#                    test_list = ["112263/2/1","112263/4/1","112263/6/1","112263/10/1"]
#                    if object_to_test in test_list:
#                            print("IN IF FOR ws_object_to_track : " + ws_object_to_track)
#                        copied_list = doi_results_map[doi]["ws_ids"][ws_id]["copied_only_objects"][object_to_test].keys()
#                        derived_list = doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"][object_to_test].keys()
#                            print(str(object_to_test) + " Copied data : " +  str(copied_list))
#                            print(str(object_to_test) + " Dereved data : " +  str(derived_list))
                    
             #doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"][ws_object_to_track][object_derived] = all_copied_only_objects_from_ws_object[object_derived]
             #doi_results_map[doi]["ws_ids"][ws_id]["copied_only_objects"][ws_object_to_track][object_copied] = all_copied_only_objects_from_ws_object[object_copied]

        #print("RESULTS : " + str(doi_results_map["ws_ids"][ws_id]))
#    print()
#    print("Objectss to check")
#    print(str(ws_objects_to_track))

#    print(str(child_parent_ws_id_lookup))
#    print("Total_copy_direct_pre_filter : " + str(total_copy_direct_pre_filter))
#    print("Total_derived_direct_pre_filter : " + str(total_derived_direct_pre_filter))
#    print("Total_copy_count_pre_filter : " + str(total_copy_count_pre_filter))
#    print("Total_derived_count_pre_filter : " + str(total_derived_count_pre_filter))
#    print("Total_copy_count : " + str(total_copy_count))
#    print("Total_derived_count : " + str(total_derived_count))

    return doi_results_map


#CHANGE BELOW these lookups all need to be for the specific WS
def get_existing_unique_derived_workspaces(db_connection, ws_id_list):
    #makes list of existing workspaces with derived data

    doi_ws_derived_workspaces_map = dict()
    cursor = db_connection.cursor()
#    get_doi_unique_workspaces_statement = ("select doi_ws_id, derived_ws_id from metrics.copy_doi_unique_workspaces;")
#    cursor.execute(get_doi_unique_workspaces_statement)
    for ws_id in ws_id_list:
        cursor.execute("select doi_ws_id, derived_ws_id from metrics.copy_doi_unique_workspaces where doi_ws_id = %s", (ws_id,))
        for row_values in cursor:
            doi_ws  = row_values[0]
            derived_ws  = row_values[1]
            if doi_ws not in doi_ws_derived_workspaces_map:
                doi_ws_derived_workspaces_map[doi_ws] = list()
            doi_ws_derived_workspaces_map[doi_ws].append(derived_ws)
    return doi_ws_derived_workspaces_map

def get_existing_unique_derived_usernames(db_connection, ws_id_list):
    #makes list of existing usernames that have derived data
    doi_ws_derived_usernames_map = dict()
    cursor = db_connection.cursor()
#    get_derived_unique_usernames_statement = ("select doi_ws_id, derived_username from metrics.copy_doi_unique_usernames;")
#    cursor.execute(get_derived_unique_usernames_statement)
    for ws_id in ws_id_list:
        cursor.execute("select doi_ws_id, derived_username from metrics.copy_doi_unique_usernames where doi_ws_id = %s", (ws_id,))
        for row_values in cursor:
            doi_ws  = row_values[0]
            derived_username  = row_values[1]
            if doi_ws not in doi_ws_derived_usernames_map:
                doi_ws_derived_usernames_map[doi_ws] = list()
            doi_ws_derived_usernames_map[doi_ws].append(derived_username)
    return doi_ws_derived_usernames_map

def get_existing_derived_objects(db_connection, ws_id_list):
#, copy_only):
    doi_object_to_derived_objects_map = dict()
    cursor = db_connection.cursor()
    for ws_id in ws_id_list:
        cursor.execute("select doi_object_id, derived_object_id from doi_externally_derived_objects where doi_ws_id = %s",(ws_id,))
        for row_values in cursor:
            doi_object_id  = row_values[0]
            derived_object_id  = row_values[1]
            if doi_object_id not in doi_object_to_derived_objects_map:
                doi_object_to_derived_objects_map[doi_object_id] = list()
            doi_object_to_derived_objects_map[doi_object_id].append(derived_object_id)
    return doi_object_to_derived_objects_map


def build_internally_derived_objects_map(db, ws_id):
    #Build derived objects where both the input and output objects are from the DOI WS
    objects_used_as_inputs = set()
    derived_objects_lookups = dict()   #input object id -> set(output_object_ids)
    #ws_obj_vers_cursor = db.workspaceObjVersions.find({"ws":ws_id,"provrefs":{ "$exists": True, "$ne": []}, "type":{"$ne":"KBaseReport.Report-2.0"}},
    ws_obj_vers_cursor = db.workspaceObjVersions.find({"ws":ws_id,"provrefs":{ "$exists": True, "$ne": []}},
                                                      {"id":1,"ver":1,"provrefs":1,"_id":0})
    for ws_obj_ver in ws_obj_vers_cursor:
        provref_inputs =  ws_obj_ver["provrefs"]
        output_ref = str( ws_id) + "/" + str(ws_obj_ver["id"]) + "/" + str(ws_obj_ver["ver"])
        for provref_input in provref_inputs:
            objects_used_as_inputs.add(provref_input)
            prov_ref_ws = provref_input.split("/")[0]
            if int(prov_ref_ws) != ws_id:
                continue
            if provref_input not in derived_objects_lookups:
                derived_objects_lookups[provref_input] = set()
            derived_objects_lookups[provref_input].add(output_ref)
#    print("derived_objects_lookups: " + str(derived_objects_lookups) + " Length of :" + str(len(derived_objects_lookups)))
#    print("objects_used_as_inputs: " + str(objects_used_as_inputs) + " Length of :" + str(len(objects_used_as_inputs)))
    return (derived_objects_lookups, objects_used_as_inputs)

def grow_internally_derived_data(internal_derived_lookup_dict, last_iteration_set, output_objects_levels_list):
    next_iteration_set = set()
    for candidate_input_id  in last_iteration_set:
        if candidate_input_id in internal_derived_lookup_dict:
#            print("candidate_input_id : " + str(candidate_input_id))
#            print("internal_derived_lookup_dict[candidate_input_id] : " + str(internal_derived_lookup_dict[candidate_input_id]))
            next_iteration_set.update(internal_derived_lookup_dict[candidate_input_id])
    if len(next_iteration_set) > 0:
        output_objects_levels_list.append(next_iteration_set)
        grow_internally_derived_data(internal_derived_lookup_dict, next_iteration_set, output_objects_levels_list)
    return output_objects_levels_list

def upload_internally_derived_objects(ws_internally_derived_dict, ws_objects_type):
    db_connection = mysql.connect(
        host=sql_host,  # "mysql1", #"localhost",
        user="metrics",  # "root",
        passwd=metrics_mysql_password,
        database="metrics",  # "datacamp"
    )
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    query = "select doi_object_input_id, doi_object_output_id from metrics.doi_internally_derived_objects"
    cursor.execute(query)
    existing_internally_derived_objects = set()
    for db_record in cursor:
        doi_input = db_record[0]
        doi_output = db_record[1]
        existing_internally_derived_objects.add(doi_input + "::" + doi_output)
    
    doi_internally_derived_prep_cursor = db_connection.cursor(prepared=True)
    doi_internally_derived_insert_statement = (
        "insert into metrics.doi_internally_derived_objects "
        "(doi_object_input_id, doi_object_output_id, doi_ws_id, steps_away,first_seen_date, input_object_type) "
        "values(%s, %s, %s, %s, now(), %s);"
    )
    for ws_id in ws_objects_type:
        for ws_obj_id in ws_objects_type[ws_id]:
            temp_key = ws_obj_id + "::" + ws_obj_id
            if temp_key in  existing_internally_derived_objects:
                continue
            else:
                #insert the missing data into doi_internally_derived_objects
                doi_internally_derived_input = (ws_obj_id, ws_obj_id, ws_id, 0,ws_objects_type[ws_id][ws_obj_id])
                doi_internally_derived_prep_cursor.execute(doi_internally_derived_insert_statement, doi_internally_derived_input)
                existing_internally_derived_objects.add(ws_obj_id + "::" +  ws_obj_id)
                    
    for ws_id in ws_internally_derived_dict:
        for input_ws_obj_id in ws_internally_derived_dict[ws_id]:
            steps_away_count = 1
            for output_ws_obj_ids_set in ws_internally_derived_dict[ws_id][input_ws_obj_id]:
                for output_ws_obj_id in output_ws_obj_ids_set:
                    temp_key = input_ws_obj_id + "::" + output_ws_obj_id
                    if temp_key in  existing_internally_derived_objects:
                        continue
                    else:
                        #insert the missing data into doi_internally_derived_objects
                        doi_internally_derived_input = (input_ws_obj_id, output_ws_obj_id, ws_id, steps_away_count, ws_objects_type[ws_id][input_ws_obj_id])
                        doi_internally_derived_prep_cursor.execute(doi_internally_derived_insert_statement, doi_internally_derived_input)
                        existing_internally_derived_objects.add(input_ws_obj_id + "::" +  output_ws_obj_id)
                steps_away_count += 1
    db_connection.commit()

#CHANGE PROCESSING PER DOI OR WS.
def upload_doi_externally_derived_data(doi_results_map, ws_owners_lookup):
    db_connection = mysql.connect(
        host=sql_host,  # "mysql1", #"localhost",
        user="metrics",  # "root",
        passwd=metrics_mysql_password,
        database="metrics",  # "datacamp"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    ws_list = doi_results_map["ws_ids"].keys()
    
    #performs inserts into 3 tables : publication_metrics, publication_unique_usernames, publication_unique_workspaces
    ws_user_start_time = time.time()
    existing_workspaces_lookup = get_existing_unique_derived_workspaces(db_connection, ws_list)
    existing_usernames_lookup = get_existing_unique_derived_usernames(db_connection, ws_list)
    print("--- Existing Usernames and WS lookup took %s seconds" % (time.time() - ws_user_start_time))
#    existing_copied_only_object_lookup = get_existing_derived_objects(db_connection, 1)
#    existing_derived_object_lookup = get_existing_derived_objects(db_connection, 0)
    existing_dervied_object_lookup_start_time = time.time()
    existing_derived_object_lookup = get_existing_derived_objects(db_connection, ws_list)
    print("--- Existing Derived Object lookup took %s seconds" % (time.time() - existing_dervied_object_lookup_start_time))
    
#    print("existing_usernames_lookup : " + str(existing_usernames_lookup))
    
    dm_prep_cursor = db_connection.cursor(prepared=True)
    doi_metrics_insert_statement = (
        "insert into metrics.copy_doi_metrics "
        "(ws_id,record_date , unique_users_count, unique_ws_ids_count, derived_object_count, copied_only_object_count, fully_derived_object_pair_counts) "
        "values(%s, now(), %s, %s, %s, %s, %s);"
    )

    duw_prep_cursor = db_connection.cursor(prepared=True)
    doi_unique_workspaces_insert_statement = (
        "insert into metrics.copy_doi_unique_workspaces "
        "(doi_ws_id, derived_ws_id, first_seen_date) "
        "values( %s, %s, now()) ")

    duu_prep_cursor = db_connection.cursor(prepared=True)
    doi_unique_usernames_insert_statement = (
        "insert into metrics.copy_doi_unique_usernames "
        "(doi_ws_id, derived_username, first_seen_date) "
        "values( %s, %s, now()) ")

    ddo_prep_cursor = db_connection.cursor(prepared=True)
    doi_externally_derived_object_insert_statement = (
        "insert into metrics.doi_externally_derived_objects "
        "(doi_ws_id, doi_object_id, derived_object_id, derived_is_copy_only,"
        "first_seen_date, derived_object_owner, derived_object_ws_id, derived_object_type) "
        "values(%s, %s, %s, %s, now(), %s, %s, %s) ")

    dtc_prep_cursor = db_connection.cursor(prepared=True)
    doi_total_counts_select_statement = (
        "select count(*) as cnt, copied_only from "
        "(select distinct dido.doi_ws_id AS doi_ws_id, dido.doi_object_input_id AS doi_object_id, "
        "dedo.derived_object_id AS derived_object_id, dedo.derived_object_owner AS derived_object_owner, "
        "dedo.derived_object_ws_id AS derived_object_ws_id, "
        "case when dido.steps_away = 0 then dedo.derived_is_copy_only else 0 end AS copied_only "
        "from metrics.doi_internally_derived_objects dido inner join metrics.doi_externally_derived_objects dedo on "
        "dido.doi_ws_id = dedo.doi_ws_id and dido.doi_object_output_id = dedo.doi_object_id "
        "where dido.doi_ws_id = %s) internal_q "
        "group by copied_only "
        )
#    doi_total_counts_select_statement = (
#        "select count(*) as cnt, copied_only "
#        "from metrics_reporting.doi_fully_derived_objects "
#        "where doi_ws_id = %s "
#        "group by copied_only ")

#    print("doi_results_map : " + str(doi_results_map))

#    for doi in doi_results_map:
    # do derived and copy counts
    for ws_id in doi_results_map["ws_ids"]:
        object_copy_count = 0
        object_derived_count = 0
        unique_users_count = len(doi_results_map["ws_ids"][ws_id]["unique_users"])
        unique_workspaces_count = len(doi_results_map["ws_ids"][ws_id]["unique_workspaces"])

        ws_user_insert_start_time = time.time()
        # doi_unique_workspaces inserts
        for derived_ws_id in doi_results_map["ws_ids"][ws_id]["unique_workspaces"]:
            needs_an_insert = False
            if ws_id not in existing_workspaces_lookup:
                needs_an_insert = True
            elif derived_ws_id not in existing_workspaces_lookup[ws_id]:
                needs_an_insert = True
            if needs_an_insert:
                duw_input = (ws_id, derived_ws_id)
                duw_prep_cursor.execute(doi_unique_workspaces_insert_statement, duw_input)

        # doi_unique_usernames inserts
        for derived_username in doi_results_map["ws_ids"][ws_id]["unique_users"]:
            needs_an_insert = False
            if ws_id not in existing_usernames_lookup:
                needs_an_insert = True
            elif derived_username not in existing_usernames_lookup[ws_id]:
                needs_an_insert = True
            if needs_an_insert:
                duu_input = (ws_id, derived_username)
                duu_prep_cursor.execute(doi_unique_usernames_insert_statement, duu_input)
        print("--- Unique Usernames and WS inserts took %s seconds" % (time.time() - ws_user_insert_start_time))

        derived_copied_insert_start_time = time.time()
        # doi_externally_derived_objects for copied only inserts
        for doi_source_object in doi_results_map["ws_ids"][ws_id]["copied_only_objects"]:
            num_copied_only_objects = len(doi_results_map["ws_ids"][ws_id]["copied_only_objects"][doi_source_object])
            object_copy_count += num_copied_only_objects
            object_derived_count += num_copied_only_objects
            for copied_only_object in doi_results_map["ws_ids"][ws_id]["copied_only_objects"][doi_source_object]:
                needs_insert = False
#                if copied_only_object not in existing_copied_only_object_lookup[doi_source_object]:
                if doi_source_object not in existing_derived_object_lookup:
                    needs_insert = True
                elif copied_only_object not in existing_derived_object_lookup[doi_source_object]:
                    needs_insert = True
                if needs_insert:
                    # get doi_ws_id, derived_ws_id, derived_object_owner
                    copied_object_type = doi_results_map["ws_ids"][ws_id]["copied_only_objects"][doi_source_object][copied_only_object]
                    doi_ws_id =  doi_source_object.split("/")[0]
                    copied_ws_id = copied_only_object.split("/")[0]
                    copied_object_owner = ws_owners_lookup[int(copied_ws_id)]
                    ddo_input = (doi_ws_id, doi_source_object, copied_only_object, 1, copied_object_owner, int(copied_ws_id), copied_object_type)
                    ddo_prep_cursor.execute(doi_externally_derived_object_insert_statement, ddo_input)
        print("--- Derived copied inserts took %s seconds" % (time.time() - derived_copied_insert_start_time))

        derived_insert_start_time = time.time()
        # doi_externally_derived_objects (some sort of input)
        for doi_source_object in doi_results_map["ws_ids"][ws_id]["derived_objects"]:
            object_derived_count += len(doi_results_map["ws_ids"][ws_id]["derived_objects"][doi_source_object])
            for derived_object in doi_results_map["ws_ids"][ws_id]["derived_objects"][doi_source_object]:
                needs_insert = False
                if doi_source_object not in existing_derived_object_lookup:
                    needs_insert = True
                elif derived_object not in existing_derived_object_lookup[doi_source_object]:
                    needs_insert = True
                if needs_insert:
                    # get doi_ws_id, derived_ws_id, derived_object_owner
                    derived_object_type = doi_results_map["ws_ids"][ws_id]["derived_objects"][doi_source_object][derived_object]
                    doi_ws_id =  doi_source_object.split("/")[0]
                    derived_ws_id = derived_object.split("/")[0]
                    derived_object_owner = ws_owners_lookup[int(derived_ws_id)]
                    ddo_input = (doi_ws_id, doi_source_object, derived_object, 0, derived_object_owner, int(derived_ws_id), derived_object_type)
                    ddo_prep_cursor.execute(doi_externally_derived_object_insert_statement, ddo_input)
        print("--- Derived inserts took %s seconds" % (time.time() - derived_insert_start_time))

        determine_fully_derived_counts_start_time = time.time()
        # get fully derived object pair_count
        dtc_input = (ws_id,)
        dtc_prep_cursor.execute(doi_total_counts_select_statement, dtc_input)
        temp_copy_only_count = 0
        temp_full_derived_minus_copy_only_count = 0
        for record in dtc_prep_cursor:
            if record[1] == 1:
                temp_copy_only_count = record[0]
            if record[1] == 0:
                temp_full_derived_minus_copy_only_count = record[0]
        if(temp_copy_only_count !=  object_copy_count):
            print("FOR WS : " + str(ws_id) + " the object_copy_count from this run (" + str(object_copy_count)  
                  + ") does not equal the view copy count (" + str(temp_copy_only_count) + ")")
        print("--- Determine fully derived counts took %s seconds" % (time.time() - determine_fully_derived_counts_start_time))
            
        # doi_metrics_insert
        dm_input = (ws_id, unique_users_count, unique_workspaces_count, object_derived_count, object_copy_count,(temp_full_derived_minus_copy_only_count + temp_copy_only_count ))
        dm_prep_cursor.execute(doi_metrics_insert_statement, dm_input)
            
    db_connection.commit()
    
def get_doi_metrics():
    client = MongoClient(mongoDB_metrics_connection + to_workspace)
    mongo_db = client.workspace

    db_connection = mysql.connect(
        host=sql_host,  # "mysql1", #"localhost",
        user="metrics",  # "root",
        passwd=metrics_mysql_password,
        database="metrics",  # "datacamp"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    master_doi_results_map  = get_dois_and_narratives()
    if (len(master_doi_results_map) == 0 ) :
        print("All DOIs have already been uploaded this month")
        exit()

    # Make lookups needed by all the DOIs and their workspaces
    copied_to_lookup_dict = build_copy_lookup(mongo_db)
    ws_owners_lookup = get_workspace_owners(mongo_db)
    master_doi_results_map = get_doi_owners_usernames(mongo_db, master_doi_results_map)
    
    #LOOP OVER ALL DOIS
    for doi in master_doi_results_map:
        doi_start_time = time.time()
        print("############################################")
        print("--- DOI : %s starteded at %s-" % (doi, str(datetime.datetime.utcnow())))

        #make local lookup for the DOIs part of the master_doi_results_map
        doi_results_map = dict()
        doi_results_map["doi_owners"] = master_doi_results_map[doi]["doi_owners"]
        doi_results_map["ws_ids"] = dict()
        for ws_id in master_doi_results_map[doi]["ws_ids"]:
            doi_results_map["ws_ids"][ws_id] = dict()
            doi_results_map["ws_ids"][ws_id]["is_parent"] = master_doi_results_map[doi]["ws_ids"][ws_id]["is_parent"]
            doi_results_map["ws_ids"][ws_id]["unique_users"] = set()
            doi_results_map["ws_ids"][ws_id]["unique_workspaces"] = set()
            doi_results_map["ws_ids"][ws_id]["copied_only_objects"] = dict()
            doi_results_map["ws_ids"][ws_id]["derived_objects"] = dict()
            doi_results_map["ws_ids"][ws_id]["object_id_downloads"] = dict()
            
        #Get object types for all of the WS in the DOI
        doi_ws_objects_types_lookup = build_doi_ws_objects_types_lookup(mongo_db, list(doi_results_map["ws_ids"].keys()))

        ws_internally_derived_dict = dict()
        for ws_id in doi_results_map["ws_ids"].keys():
            ws_internally_derived_dict[ws_id] = dict()
            (internal_derived_lookup_dict, inputs_objects_set) = build_internally_derived_objects_map(mongo_db, ws_id)
            for input_object_id in inputs_objects_set:
                output_objects_levels_list = list()
                internally_derived_data = grow_internally_derived_data(internal_derived_lookup_dict, set([input_object_id]),output_objects_levels_list)
                ws_internally_derived_dict[ws_id][input_object_id] = internally_derived_data
        upload_internally_derived_objects(ws_internally_derived_dict, doi_ws_objects_types_lookup)
        print("--- DOI : %s  WS: %s finished internally derived took %s-secpnds" % (doi, str(ws_id),time.time() - doi_start_time))
        
        #procss stats per DOI
        doi_results_map = determine_doi_statistics(mongo_db, doi_results_map, copied_to_lookup_dict, ws_owners_lookup, doi_ws_objects_types_lookup)
        print("--- DOI : %s  finished statistics determination at %s-" % (doi, time.time() - doi_start_time))
        upload_doi_externally_derived_data(doi_results_map, ws_owners_lookup)
        print("--- DOI : %s  finished externally derived_upload tooke  %s- seconds" % (doi, time.time() - doi_start_time))
        print("--- DOI : %s containting workspaces %s took Total time %s seconds ---" % (doi, str(doi_results_map["ws_ids"].keys()),time.time() - doi_start_time))
#    exit()




    # Take care of overall metrics and externally derived objects
#    doi_results_map = get_doi_owners_usernames(db, doi_results_map)

    #print(str(doi_results_map))
#    exit()
#    upload_doi_internally_derived_objects(db, doi_ws_objects_types_lookup) 


#    print("FINAL MAP: " + str(doi_results_map))



print("############################################")
start_time = time.time()
get_doi_metrics()
#print("max_in_string_length : " + str(max_in_string_length))
#print("max_list_length : " + str(max_list_length))
print("##############################################")
print("--- Total TIME for all DOIs %s seconds ---" % (time.time() - start_time))

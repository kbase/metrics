from pymongo import MongoClient
#from pymongo import ReadPreference
import os
import mysql.connector as mysql
import requests
import populate_downloading_apps
from arango import ArangoClient
#import time
from datetime import date
#from datetime import datetime
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

def get_dois_and_narratives(cursor):
    #creates a dict of DOIs as keys to values of a list of WS_IDS to look at.
    #If the ws_id value list is a single element the DOI is associated with the WS_ID
    #If the ws_id value list has multiple ws_ids the first ws_id is the parent organizining ws_id,
    #the remainder in the list are childredn ws_ids.
    query = "select doi_url, ws_id, is_parent_ws from metrics.copy_doi_ws_map";
#    query = "select doi_url, ws_id, is_parent_ws from metrics.doi_ws_map";
    cursor.execute(query)
    doi_results_map = dict()
    for row_values in cursor:
#        if row_values[1] != 86723:
#            continue
        doi = row_values[0]
        ws_id = row_values[1]
        is_parent = row_values[2]
        if doi not in doi_results_map:
            doi_results_map[doi] = dict()
            doi_results_map[doi]["doi_owners"] = set()
            doi_results_map[doi]["ws_ids"] = dict()
        doi_results_map[doi]["ws_ids"][ws_id] = dict()
        doi_results_map[doi]["ws_ids"][ws_id]["is_parent"] = is_parent
        doi_results_map[doi]["ws_ids"][ws_id]["unique_users"] = set()
        doi_results_map[doi]["ws_ids"][ws_id]["unique_workspaces"] = set()
        doi_results_map[doi]["ws_ids"][ws_id]["copied_only_objects"] = dict() 
        doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"] = dict() 
        doi_results_map[doi]["ws_ids"][ws_id]["object_id_downloads"] = dict()

#    print(str(doi_results_map))
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


def quick_parent_lookup(doi_results_map):
    #returns lookup of children WS to find the parent WS

    child_parent_ws_id_lookup = dict()
    for doi in doi_results_map:
        parent_ws_id = None
        child_ws_list = list()
        for ws_id in doi_results_map[doi]["ws_ids"]:
            if doi_results_map[doi]["ws_ids"][ws_id]["is_parent"] == 1:
                parent_ws_id =ws_id
            else:
                child_ws_list.append(ws_id)
        if parent_ws_id is None:
            # Raise an error should not be the case.  Means the WS data is incorrect
            raise ValueError("The data in doi_ws_map is not set up properly every doi must have 1 parent ws (even if no children).")
        for child_ws_id in child_ws_list:
            child_parent_ws_id_lookup[child_ws_id] = parent_ws_id
    return child_parent_ws_id_lookup

def grow_derived_set_mongo(db, doi_ws_id, copied_to_lookup_dict, master_set, last_iteration_set, copy_only):
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
#    print("Next Iteration SET PRE:" + str(next_iteration_set))
    if copy_only == 0 :
        #useing ws_obj_versions to lookup
        #DOING ALL PREVIOUS ITERATION IN one call
#        global max_in_string_length
#        global max_list_length
#        temp_in_string_length = len(''.join(list(last_iteration_set)))
#        if temp_in_string_length > max_in_string_length:
#            max_in_string_length = temp_in_string_length
#            max_list_length = len(list(last_iteration_set))
        # Mongo's "in" clause has a limit of 16.6 MB. Each character is 1 byte
        # The current limit is set for 500K which totals about 6MB with the current UPAs length
        # This gives us considerable cushion. But the 500K can be lowered if need in the future
        # But I would be surprised if UPAs get over twice the current size..
        list_of_last_iteration = list(last_iteration_set)
        last_list_length = len(list_of_last_iteration)
        list_chunk = 500000
        list_chunk_counter = 0
        while list_chunk_counter < last_list_length:
            sub_last_iteration_list = list_of_last_iteration[list_chunk_counter:(list_chunk_counter + list_chunk)]
            ws_obj_cursor = db.workspaceObjVersions.find({"provrefs":{"$in":sub_last_iteration_list}},{"ws":1,"id":1,"ver":1,"_id":0}) #"type":1, "provrefs":1,"_id":0})
            for ws_obj_item in ws_obj_cursor:
                if int(ws_obj_item["ws"]) != doi_ws_id:
                    derived_ws_obj_id = str(ws_obj_item["ws"]) + "/" + str(ws_obj_item["id"]) + "/" + str(ws_obj_item["ver"])
                    next_iteration_set.add(derived_ws_obj_id)
            list_chunk_counter += list_chunk
#FOR SINGLE CALLS TAKES 687 mins
#        for object_ws_obj_id in last_iteration_set:
#            ws_obj_cursor = db.workspaceObjVersions.find({"provrefs":object_ws_obj_id},{"ws":1,"id":1,"ver":1,"_id":0}) #"type":1, "provrefs":1,"_id":0})
#            for ws_obj_item in ws_obj_cursor:
#                if int(ws_obj_item["ws"]) != doi_ws_id:
#                    derived_ws_obj_id = str(ws_obj_item["ws"]) + "/" + str(ws_obj_item["id"]) + "/" + str(ws_obj_item["ver"])
#                    next_iteration_set.add(derived_ws_obj_id)
    if len(next_iteration_set) > 0:
#        print("master set pre append: " + str(master_set))
        master_set = master_set | next_iteration_set
#        print("master set post append: " + str(master_set))
        master_set = grow_derived_set_mongo(db,doi_ws_id, copied_to_lookup_dict, master_set, next_iteration_set, copy_only)
#        print("master set post function call: " + str(master_set))
    return master_set

        
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
        print("Next Iteration SET POST:" + str(next_iteration_set))

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
               

def determine_doi_unique_users_and_ws_ids(db, doi_results_map, copied_to_lookup_dict, ws_owners_lookup):
    #Populates the doi_results_map with the
    #unique set of users to ws_ids
    child_parent_ws_id_lookup = quick_parent_lookup(doi_results_map)
#    print("copied_to_lookup_dict: " + str(copied_to_lookup_dict))
    #ws_objectss_to_track = dict()
    for doi in doi_results_map:
#        if doi != 'https://doi.org/10.25982/44746.21/1635640' and doi != "https://doi.org/10.25982/54100.27/1635639":
#        if doi != 'pretned_doi':
#        if doi != 'https://www.doi.org/10.25982/86723.65/1778009':
#            continue
        doi_owners_usernames = doi_results_map[doi]["doi_owners"]
        for ws_id in doi_results_map[doi]["ws_ids"]:
#            if ws_id != 53247:
#                continue
#            print("DOI: " + doi)
            #print("WS ID BEING USED:" + str(ws_id))
            ws_objects_to_track = dict()
            ws_objects_to_track = get_objects_for_ws(db, ws_id)
            #print("WS_OBJECTS_TO_TRACK: " + str(ws_objects_to_track))
            parent_ws_id = None
            if ws_id in child_parent_ws_id_lookup:
                parent_ws_id = child_parent_ws_id_lookup[ws_id]

#NEW WAY NOT WORKING                
#            for ws_object_to_track in ws_objects_to_track:
#                all_derived_objects_from_ws_object = grow_derived_list(copied_to_lookup_dict, [ws_object_to_track], [ws_object_to_track])
#                print("All_dervied_objects_from_ws: " + str(all_derived_objects_from_ws_object))
#                for object_derived in all_derived_objects_from_ws_object:
#                    temp_derived_object_ws_id = object_derived.split("/",1)[0]
#                    derived_object_ws_id = int(temp_derived_object_ws_id)
#                    if ws_owners_lookup[derived_object_ws_id] not in doi_owners_usernames:
#                        doi_results_map[doi]["ws_ids"][ws_id]["unique_users"].add(ws_owners_lookup[derived_object_ws_id])
#                        doi_results_map[doi]["ws_ids"][ws_id]["unique_workspaces"].add(derived_object_ws_id)
#                        if ws_id in child_parent_ws_id_lookup:
#                            parent_ws_id = child_parent_ws_id_lookup[ws_id]
#                            doi_results_map[doi]["ws_ids"][parent_ws_id]["unique_users"].add(ws_owners_lookup[derived_object_ws_id])
#                            doi_results_map[doi]["ws_ids"][parent_ws_id]["unique_workspaces"].add(derived_object_ws_id)

#TRYING NEW WAY
            #print("ws_objects_to_track :: " + str(ws_objects_to_track))
            for ws_object_to_track in ws_objects_to_track:
                all_copied_only_objects_from_ws_object = grow_derived_set_mongo(db,ws_id, copied_to_lookup_dict, set([ws_object_to_track]), set([ws_object_to_track]),1)
                #print("all_copied_only_objects_from_ws_object : " + ws_object_to_track + ":::" + str(all_copied_only_objects_from_ws_object))
#                all_derived_objects_from_ws_object = list()
                all_derived_objects_from_ws_object = grow_derived_set_mongo(db,ws_id, copied_to_lookup_dict, set([ws_object_to_track]), set([ws_object_to_track]),0)
                #print("ALL DERIVED OBJECT LIST from ws object: " + str(ws_object_to_track) + " :: " + str(all_derived_objects_from_ws_object))

                for object_copied in all_copied_only_objects_from_ws_object:
                    temp_copied_object_ws_id = object_copied.split("/",1)[0]
                    copied_object_ws_id = int(temp_copied_object_ws_id)
#                   print("copied WS : " + str( copied_object_ws_id) + "  The copied owner lookup: " + str(ws_owners_lookup[copied_object_ws_id]))
                    if ws_owners_lookup[copied_object_ws_id] not in doi_owners_usernames:
                        doi_results_map[doi]["ws_ids"][ws_id]["unique_users"].add(ws_owners_lookup[copied_object_ws_id])
                        doi_results_map[doi]["ws_ids"][ws_id]["unique_workspaces"].add(copied_object_ws_id)
                        if ws_object_to_track not in doi_results_map[doi]["ws_ids"][ws_id]["copied_only_objects"]:
                            doi_results_map[doi]["ws_ids"][ws_id]["copied_only_objects"][ws_object_to_track] = set()
                        doi_results_map[doi]["ws_ids"][ws_id]["copied_only_objects"][ws_object_to_track].add(object_copied)

                        if ws_id in child_parent_ws_id_lookup:
                            parent_ws_id = child_parent_ws_id_lookup[ws_id]
                            doi_results_map[doi]["ws_ids"][parent_ws_id]["unique_users"].add(ws_owners_lookup[copied_object_ws_id])
                            doi_results_map[doi]["ws_ids"][parent_ws_id]["unique_workspaces"].add(copied_object_ws_id)
                            if ws_object_to_track not in doi_results_map[doi]["ws_ids"][parent_ws_id]["copied_only_objects"]:
                                doi_results_map[doi]["ws_ids"][parent_ws_id]["copied_only_objects"][ws_object_to_track] = set()
                            doi_results_map[doi]["ws_ids"][parent_ws_id]["copied_only_objects"][ws_object_to_track].add(object_copied)

                for object_derived in all_derived_objects_from_ws_object:
                    temp_derived_object_ws_id = object_derived.split("/",1)[0]
                    derived_object_ws_id = int(temp_derived_object_ws_id)
    #               print("copied WS : " + str( copied_object_ws_id) + "  The copied owner lookup: " + str(ws_owners_lookup[copied_object_ws_id]))
                    if object_derived not in all_copied_only_objects_from_ws_object:
                        #means the obect used as input at one point
                        if ws_owners_lookup[derived_object_ws_id] not in doi_owners_usernames:
                            doi_results_map[doi]["ws_ids"][ws_id]["unique_users"].add(ws_owners_lookup[derived_object_ws_id])
                            doi_results_map[doi]["ws_ids"][ws_id]["unique_workspaces"].add(derived_object_ws_id)
                            if ws_object_to_track not in doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"]:
                                doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"][ws_object_to_track] = set()
                            doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"][ws_object_to_track].add(object_derived)

                            if ws_id in child_parent_ws_id_lookup:
                                parent_ws_id = child_parent_ws_id_lookup[ws_id]
                                doi_results_map[doi]["ws_ids"][parent_ws_id]["unique_users"].add(ws_owners_lookup[derived_object_ws_id])
                                doi_results_map[doi]["ws_ids"][parent_ws_id]["unique_workspaces"].add(derived_object_ws_id)
                                if ws_object_to_track not in doi_results_map[doi]["ws_ids"][parent_ws_id]["derived_objects"]:
                                    doi_results_map[doi]["ws_ids"][parent_ws_id]["derived_objects"][ws_object_to_track] = set()
                                doi_results_map[doi]["ws_ids"][parent_ws_id]["derived_objects"][ws_object_to_track].add(object_derived)

###########                
#OLD WAY WORKING                
#            all_derived_objects_from_ws_list = grow_derived_list(copied_to_lookup_dict, [], ws_objects_to_track)
#            print("ALL DERIVED OBJECTS LIST: " + str(all_derived_objects_from_ws_list))
#            for object_derived in all_derived_objects_from_ws_list:
#                temp_derived_object_ws_id = object_derived.split("/",1)[0]
#                derived_object_ws_id = int(temp_derived_object_ws_id)
#                print("copied WS : " + str( copied_object_ws_id) + "  The copied owner lookup: " + str(ws_owners_lookup[copied_object_ws_id]))
#                if ws_owners_lookup[derived_object_ws_id] not in doi_owners_usernames:
#                    doi_results_map[doi]["ws_ids"][ws_id]["unique_users"].add(ws_owners_lookup[derived_object_ws_id])
#                    doi_results_map[doi]["ws_ids"][ws_id]["unique_users"].add(ws_owners_lookup[derived_object_ws_id])
#                    doi_results_map[doi]["ws_ids"][ws_id]["unique_workspaces"].add(derived_object_ws_id)
#                    if ws_id in child_parent_ws_id_lookup:
#                        parent_ws_id = child_parent_ws_id_lookup[ws_id]
#                        doi_results_map[doi]["ws_ids"][parent_ws_id]["unique_users"].add(ws_owners_lookup[derived_object_ws_id])
#                        doi_results_map[doi]["ws_ids"][parent_ws_id]["unique_workspaces"].add(derived_object_ws_id)
                        
            # NOW GET THE DOWNLOAD INFORMATION FOR ANY OF THE DOI WS OBJECTS
            # OR ANY OF THEIR RESULTING COPIED OBJECTS.
###            for ws_object_to_track in ws_objects_to_track:
###                objects_to_check_if_downloaded = grow_derived_list(copied_to_lookup_dict, [ws_object_to_track],[ws_object_to_track])
#                print(str(objects_to_check_if_downloaded))
                # See if any these IDs have been downloaded

                # If so add to the 'object_id_downloads': {}}
                # {ws_object_to_trac(root object id)=>{Actual object id downloaded(could be root or copied child) => [List of username that copied (not part of author list]}}
                
                
#            print("RESULTS : " + str(doi_results_map[doi]["ws_ids"][ws_id]))
#    print()
#    print("Objectss to check")
#    print(str(ws_objects_to_track))

#    print(str(child_parent_ws_id_lookup))
    return doi_results_map

def get_existing_unique_derived_workspaces(db_connection):
    #makes list of existing workspaces with derived data

    doi_ws_derived_workspaces_map = dict()
    cursor = db_connection.cursor()
#    get_doi_unique_workspaces_statement = ("select published_ws_id, copied_ws_id from metrics.doi_unique_workspaces;")
    get_doi_unique_workspaces_statement = ("select doi_ws_id, derived_ws_id from metrics.copy_doi_unique_workspaces;")
    cursor.execute(get_doi_unique_workspaces_statement)
    for row_values in cursor:
        doi_ws  = row_values[0]
        derived_ws  = row_values[1]
#        if doi_ws != 86723:
#            continue
        if doi_ws not in doi_ws_derived_workspaces_map:
            doi_ws_derived_workspaces_map[doi_ws] = list()
        doi_ws_derived_workspaces_map[doi_ws].append(derived_ws)
    return doi_ws_derived_workspaces_map

def get_existing_unique_derived_usernames(db_connection):
    #makes list of existing usernames that have derived data
    doi_ws_derived_usernames_map = dict()
    cursor = db_connection.cursor()
    get_derived_unique_usernames_statement = ("select doi_ws_id, derived_username from metrics.copy_doi_unique_usernames;")
    cursor.execute(get_derived_unique_usernames_statement)
    for row_values in cursor:
        doi_ws  = row_values[0]
        derived_username  = row_values[1]
#        if doi_ws != 86723:
#            continue
#        print("doi_ws: " + str(doi_ws) + "  derived_username : " + derived_username) 
        if doi_ws not in doi_ws_derived_usernames_map:
            doi_ws_derived_usernames_map[doi_ws] = list()
        doi_ws_derived_usernames_map[doi_ws].append(derived_username)
#        print("map for : " + str(doi_ws) + "  ::: " + str(doi_ws_derived_usernames_map[doi_ws]))
#    print("doi_ws_derived_usernames_map : " + str(doi_ws_derived_usernames_map))
    return doi_ws_derived_usernames_map

def get_existing_derived_objects(db_connection):
#, copy_only):
    doi_object_to_derived_objects_map = dict()
    cursor = db_connection.cursor()
#    doi_do_prep_cursor = db_connection.cursor(prepared=True)
    doi_object_to_derived_objects_query = (
        "select doi_object_id, derived_object_id "
        "from doi_externally_derived_objects ")
#        "where derived_is_copy_only = %s")
    cursor.execute(doi_object_to_derived_objects_query)
#    doi_object_to_derived_objects_input = (copy_only)
#    doi_do_prep_cursor.execute(doi_object_to_derived_objects_query, doi_object_to_derived_objects_input)

#    for row_values in doi_do_prepcursor:
    for row_values in cursor:
        doi_object_id  = row_values[0]
        derived_object_id  = row_values[1]
        if doi_object_id not in doi_object_to_derived_objects_map:
            doi_object_to_derived_objects_map[doi_object_id] = list()
        doi_object_to_derived_objects_map[doi_object_id].append(derived_object_id)
    return doi_object_to_derived_objects_map

def upload_doi_data(doi_results_map, ws_owners_lookup):
    db_connection = mysql.connect(
        host=sql_host,  # "mysql1", #"localhost",
        user="metrics",  # "root",
        passwd=metrics_mysql_password,
        database="metrics",  # "datacamp"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)
    
    #performs inserts into 3 tables : publication_metrics, publication_unique_usernames, publication_unique_workspaces
    existing_workspaces_lookup = get_existing_unique_derived_workspaces(db_connection)
    existing_usernames_lookup = get_existing_unique_derived_usernames(db_connection)
#    existing_copied_only_object_lookup = get_existing_derived_objects(db_connection, 1)
#    existing_derived_object_lookup = get_existing_derived_objects(db_connection, 0)
    existing_derived_object_lookup = get_existing_derived_objects(db_connection)

    
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
        "first_seen_date, derived_object_owner, derived_object_ws_id) "
        "values(%s, %s, %s, %s, now(), %s, %s) ")

    dtc_prep_cursor = db_connection.cursor(prepared=True)
    doi_total_counts_select_statement = (
        "select count(*) as cnt, copied_only "
        "from metrics_reporting.doi_fully_derived_objects "
        "where doi_ws_id = %s "
        "group by copied_only ")

#    print("doi_results_map : " + str(doi_results_map))

    for doi in doi_results_map:
        # do derived and copy counts
        object_copy_count = 0
        object_derived_count = 0
        for ws_id in doi_results_map[doi]["ws_ids"]:
            for copied_only_object in doi_results_map[doi]["ws_ids"][ws_id]["copied_only_objects"]:
                num_copied_only_objects = len(doi_results_map[doi]["ws_ids"][ws_id]["copied_only_objects"][copied_only_object])
                object_copy_count += num_copied_only_objects
                object_derived_count += num_copied_only_objects
            for derived_object in doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"]:
                object_derived_count += len(doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"][derived_object])
        
        for ws_id in doi_results_map[doi]["ws_ids"]:
            unique_users_count = len(doi_results_map[doi]["ws_ids"][ws_id]["unique_users"])
            unique_workspaces_count = len(doi_results_map[doi]["ws_ids"][ws_id]["unique_workspaces"])

            # doi_unique_workspaces inserts
            for derived_ws_id in doi_results_map[doi]["ws_ids"][ws_id]["unique_workspaces"]:
                needs_an_insert = False
                if ws_id not in existing_workspaces_lookup:
                    needs_an_insert = True
                elif derived_ws_id not in existing_workspaces_lookup[ws_id]:
                    needs_an_insert = True
                if needs_an_insert:
                    duw_input = (ws_id, derived_ws_id)
                    duw_prep_cursor.execute(doi_unique_workspaces_insert_statement, duw_input)

            # doi_unique_usernames inserts
            for derived_username in doi_results_map[doi]["ws_ids"][ws_id]["unique_users"]:
                needs_an_insert = False
                if ws_id not in existing_usernames_lookup:
                    needs_an_insert = True
                elif derived_username not in existing_usernames_lookup[ws_id]:
                    needs_an_insert = True
                if needs_an_insert:
                    duu_input = (ws_id, derived_username)
                    duu_prep_cursor.execute(doi_unique_usernames_insert_statement, duu_input)

            # doi_externally_derived_objects for copied only inserts
            for doi_source_object in doi_results_map[doi]["ws_ids"][ws_id]["copied_only_objects"]:
                for copied_only_object in doi_results_map[doi]["ws_ids"][ws_id]["copied_only_objects"][doi_source_object]:
                    needs_insert = False
#                    if copied_only_object not in existing_copied_only_object_lookup[doi_source_object]:

                    if doi_source_object not in existing_derived_object_lookup:
                        needs_insert = True
                    elif copied_only_object not in existing_derived_object_lookup[doi_source_object]:
                        needs_insert = True
                    if needs_insert:
                        # get doi_ws_id, derived_ws_id, derived_object_owner
                        doi_ws_id =  doi_source_object.split("/")[0]
                        copied_ws_id = copied_only_object.split("/")[0]
                        copied_object_owner = ws_owners_lookup[int(copied_ws_id)]
                        ddo_input = (doi_ws_id, doi_source_object, copied_only_object,1,copied_object_owner,int(copied_ws_id))
                        ddo_prep_cursor.execute(doi_externally_derived_object_insert_statement, ddo_input)
                        
            # doi_externally_derived_objects (some sort of input)
            for doi_source_object in doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"]:
                for derived_object in doi_results_map[doi]["ws_ids"][ws_id]["derived_objects"][doi_source_object]:
                    needs_insert = False
                    if doi_source_object not in existing_derived_object_lookup:
                        needs_insert = True
                    elif derived_object not in existing_derived_object_lookup[doi_source_object]:
                        needs_insert = True
                    if needs_insert:
                        # get doi_ws_id, derived_ws_id, derived_object_owner
                        doi_ws_id =  doi_source_object.split("/")[0]
                        derived_ws_id = derived_object.split("/")[0]
                        derived_object_owner = ws_owners_lookup[int(derived_ws_id)]
                        ddo_input = (doi_ws_id, doi_source_object, derived_object,0,derived_object_owner,int(derived_ws_id))
                        ddo_prep_cursor.execute(doi_externally_derived_object_insert_statement, ddo_input)

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
            # doi_metrics_insert
            dm_input = (ws_id, unique_users_count, unique_workspaces_count, object_derived_count, object_copy_count,(temp_full_derived_minus_copy_only_count + temp_copy_only_count ))
            dm_prep_cursor.execute(doi_metrics_insert_statement, dm_input)
            
    db_connection.commit()

def get_doi_metrics():
    client = MongoClient(mongoDB_metrics_connection + to_workspace)
    db = client.workspace

    db_connection = mysql.connect(
        host=sql_host,  # "mysql1", #"localhost",
        user="metrics",  # "root",
        passwd=metrics_mysql_password,
        database="metrics",  # "datacamp"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    # CHECK IF THIS WAS DONE THIS MONTH ALREADY. IT IS MEANT TO BE RUN ONCE PER MONTH (IDEALLY THE FIRST OF THE MONTH)
    today = date.today()
    current_month = str(today.year) + "-" + today.strftime("%m")

    # NOTE THIS IS ONE OF THREE TABLES THAT NEED TO BE DELETED FROM IS RAN ON A SMALL SAMPLE OF WORKSPACES
    # SO DO NOT HAVE DOUBLE ENTRIES IN THE SAME MONTH
    # metrics.users_workspace_object_counts, metrics.workspace_object_counts, metrics.workspaces
    query = "select DATE_FORMAT(max(record_date),'%Y-%m') from metrics.doi_metrics"
    cursor.execute(query)
    for db_date in cursor:
        db_date_month = db_date[0]
#        if db_date_month == current_month:
#            print("THE DOI METRICS HAS BEEN RUN THIS MONTH. THE PROGRAM WILL EXIT")
#            exit()
#        else:
#            print("THE DOI METRICS HAS NOT BEEN RUN THIS MONTH, WE WILL RUN THE PROGRAM")

    copied_to_lookup_dict = build_copy_lookup(db)
    ws_owners_lookup = get_workspace_owners(db)

    doi_results_map = get_dois_and_narratives(cursor)
    doi_results_map = get_doi_owners_usernames(db, doi_results_map)
    doi_results_map = determine_doi_unique_users_and_ws_ids(db, doi_results_map, copied_to_lookup_dict, ws_owners_lookup)
    #print(str(doi_results_map))
#    exit()
    upload_doi_data(doi_results_map, ws_owners_lookup)

#    print("FINAL MAP: " + str(doi_results_map))



print("############################################")
start_time = time.time()
get_doi_metrics()
#print("max_in_string_length : " + str(max_in_string_length))
#print("max_list_length : " + str(max_list_length))
print("--- Total TIME %s seconds ---" % (time.time() - start_time))

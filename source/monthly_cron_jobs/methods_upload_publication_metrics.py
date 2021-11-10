from pymongo import MongoClient
from pymongo import ReadPreference
from biokbase.workspace.client import Workspace
from biokbase.service.Client import Client as ServiceClient
import json as _json
import os
import mysql.connector as mysql
import requests
import time
from datetime import date
from datetime import datetime

# import pprint
requests.packages.urllib3.disable_warnings()

# NOTE get_user_info_from_auth2 sets up the initial dict.
# The following functions update certain fields in the dict.
# So get_user_info_from_auth2 must be called before get_internal_users and get_user_orgs_count

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
mongoDB_metrics_connection = os.environ["MONGO_PATH"]

sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]

ws_url = os.environ["WS_URL"]
ws_user_token = os.environ["METRICS_WS_USER_TOKEN"]
to_workspace = os.environ["WRK_SUFFIX"]

def build_copy_lookup(db):
    """
    builds a dict of keys of source_object_id and values of set of copied_object_ids
    """    
    copied_genome_count = 0
    copied_to_lookup_dict = dict()
#    ws_obj_vers_cursor = db.workspaceObjVersions.find( {"copied" : {"$ne": null}},{"copied":1, "ws":1, "id":1, "ver":1, "type":1,"_id":0})
    ws_obj_vers_cursor = db.workspaceObjVersions.find( {"copied" : {"$ne": None}},{"copied":1, "ws":1, "id":1, "ver":1, "type":1,"_id":0})
    for ws_obj_ver in ws_obj_vers_cursor:
        # check if it is a genome type
        object_type_full = ws_obj_ver["type"]
        (object_type, object_spec_version) = object_type_full.split("-")
        if object_type != "KBaseGenomes.Genome":
            continue
        copied_genome_count += 1
        full_obj_id_of_copy = str(ws_obj_ver["ws"]) + "/" + str(ws_obj_ver["id"]) + "/" + str(ws_obj_ver["ver"]) 
        if ws_obj_ver["copied"] not in copied_to_lookup_dict:
            copied_to_lookup_dict[ws_obj_ver["copied"]] = list()
        copied_to_lookup_dict[ws_obj_ver["copied"]].append(full_obj_id_of_copy)
#    print(str(copied_to_lookup_dict))
    print("Total genome sources copied: " + str(len(copied_to_lookup_dict)))
    print("Total resulting copies: " + str(copied_genome_count))
#    copy_count_dict = dict()
#    for copied_genome in copied_to_lookup_dict:
#        temp_length = str(len(copied_to_lookup_dict[copied_genome]))
#        if temp_length not in copy_count_dict:
#            copy_count_dict[temp_length] = 1
#        else:
#            copy_count_dict[temp_length] += 1
#    for key, value in sorted(copy_count_dict.items(), key=lambda item: int(item[0])):
#        print(key, value)
    return copied_to_lookup_dict

def get_workspace_owners(db):
    """
    builds a dict of keys of ws_id and values of usernames who own that ws.
    """
    ws_owners_lookup = dict()
    ws_cursor = db.workspaces.find({},{"ws":1, "owner":1, "_id":0})
    for ws_item in ws_cursor:
        ws_owners_lookup[ws_item["ws"]] = ws_item["owner"]
    print("Ws owners: " + str(ws_owners_lookup))
    return ws_owners_lookup

def get_dois_and_narratives(cursor):
    """
    creates a dict of DOIs as keys to values of a list of WS_IDS to look at.
    If the ws_id value list is a single element the DOI is associated with the WS_ID
    If the ws_id value list has multiple ws_ids the first ws_id is the parent organizining ws_id,
    the remainder in the list are childredn ws_ids.
    """
    query = "select doi_url, ws_id, is_parent_ws from metrics.doi_ws_map";
    cursor.execute(query)
    doi_results_map = dict()
    for row_values in cursor:
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

    print(str(doi_results_map))
    return doi_results_map

def get_dois_participant_usernames(db, doi_results_map):
    """
    creates a set of unique usernames associated with the DOI
    Any copies and usernames will not be counted if they are part of that list.
    """
    for doi in doi_results_map:
        for ws_id in doi_results_map[doi]["ws_ids"]:
            ws_perm_cursor = db.workspaceACLs.find({"id":ws_id},{"user":1, "perm":1, "_id":0})
            for ws_perm in ws_perm_cursor:
                if ws_perm["perm"] > 10:
                    doi_results_map[doi]["doi_owners"].add(ws_perm["user"])
    print(str(doi_results_map))
    return doi_results_map

def get_genomes_for_ws(db, ws_id):
    """
    gets a list of ws_references from the passed WS_ID
    """
    genomes_to_check_copies_list = list()
    ws_objs_cursor = db.workspaceObjVersions.find({"ws":ws_id},{"type":1, "id":1, "ver":1,"_id":0})
    for ws_obj in ws_objs_cursor:
        full_obj_type = ws_obj["type"]
        core_type,obj_type_ver = full_obj_type.split('-',1)
        if core_type == "KBaseGenomes.Genome":
            obj_id = str(ws_id) + "/" + str(ws_obj["id"]) + "/" + str(ws_obj["ver"])
            genomes_to_check_copies_list.append(obj_id)
    return genomes_to_check_copies_list

def quick_parent_lookup(doi_results_map):
    """
    returns lookup of children WS to find the parent WS
    """
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
            i = 1
        for child_ws_id in child_ws_list:
            child_parent_ws_id_lookup[child_ws_id] = parent_ws_id
    return child_parent_ws_id_lookup

def get_publication_metrics():
    client = MongoClient(mongoDB_metrics_connection + to_workspace)
    db = client.workspace

    copied_to_lookup_dict = build_copy_lookup(db)
    ws_owners_lookup = get_workspace_owners(db)

    db_connection = mysql.connect(
        host=sql_host,  # "mysql1", #"localhost",
        user="metrics",  # "root",
        passwd=metrics_mysql_password,
        database="metrics",  # "datacamp"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    doi_results_map = get_dois_and_narratives(cursor)
    doi_results_map = get_dois_participant_usernames(db, doi_results_map)

    child_parent_ws_id_lookup = quick_parent_lookup(doi_results_map)
    
    ws_genomes_to_track = dict()
    for doi in doi_results_map:
        for ws_id in doi_results_map[doi]["ws_ids"]:
            print("WS ID BEING USED:" + str(ws_id))
            ws_genomes_to_track[ws_id] = get_genomes_for_ws(db, ws_id)
    print()
    print("Genomes to check")
    print(str(ws_genomes_to_track))

    print(str(child_parent_ws_id_lookup))
            
            
get_publication_metrics()

    

from pymongo import MongoClient
#from pymongo import ReadPreference
import os
import mysql.connector as mysql
import requests
#import time
from datetime import date
#from datetime import datetime

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
        object_spec_version = "don nothing with this"
        if object_type != "KBaseGenomes.Genome":
            continue
        copied_genome_count += 1
        full_obj_id_of_copy = str(ws_obj_ver["ws"]) + "/" + str(ws_obj_ver["id"]) + "/" + str(ws_obj_ver["ver"])
        if ws_obj_ver["copied"] not in copied_to_lookup_dict:
            copied_to_lookup_dict[ws_obj_ver["copied"]] = list()
        copied_to_lookup_dict[ws_obj_ver["copied"]].append(full_obj_id_of_copy)
#    print(str(copied_to_lookup_dict))
#    print("Total genome sources copied: " + str(len(copied_to_lookup_dict)))
#    print("Total resulting copies: " + str(copied_genome_count))
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
#    print("Ws owners: " + str(ws_owners_lookup))
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

#    print(str(doi_results_map))
    return doi_results_map

def get_doi_owners_usernames(db, doi_results_map):
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
#    print(str(doi_results_map))
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
        obj_type_ver = "Do nothing with this"
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
            raise ValueError("The data in doi_ws_map is not set up properly every doi must have 1 parent ws (even if no children).")
        for child_ws_id in child_ws_list:
            child_parent_ws_id_lookup[child_ws_id] = parent_ws_id
    return child_parent_ws_id_lookup

def grow_copied_list(copied_to_lookup_dict, master_list, last_iteration_list):
    """
    grows the list of copied genomes for a WS
    returns the master_list and next iteration list
    """
    next_iteration_list = list()
#    print("Last Iteration LIST:" + str(last_iteration_list))
    for genome_ws_obj_id in last_iteration_list:
#        print("Result of copied lookup for " + str(genome_ws_obj_id) + " : " + str(copied_to_lookup_dict.get(genome_ws_obj_id)))
        if genome_ws_obj_id in copied_to_lookup_dict:
            next_iteration_list = next_iteration_list + copied_to_lookup_dict[genome_ws_obj_id]
#    print("Next Iteration LIST:" + str(next_iteration_list))
    if len(next_iteration_list) > 0:
#        print("master list pre append: " + str(master_list))
        master_list = master_list + next_iteration_list
#        print("master list post append: " + str(master_list))
        master_list = grow_copied_list(copied_to_lookup_dict, master_list, next_iteration_list)
#        print("master list post function call: " + str(master_list))
    return master_list
#    else:
        # no new copies time to return master list
#        print("master list in else: " + str(master_list))
#        return master_list

def determine_publication_unique_users_and_ws_ids(db, doi_results_map, copied_to_lookup_dict, ws_owners_lookup):
    """
    Populates the doi_results_map with the
    unique set of users to ws_ids
    """
    child_parent_ws_id_lookup = quick_parent_lookup(doi_results_map)
    #ws_genomes_to_track = dict()
    for doi in doi_results_map:
        #if doi != 'https://doi.org/10.25982/44746.21/1635640' and doi != "https://doi.org/10.25982/54100.27/1635639":
#        if doi != 'pretned_doi':
#            continue
        doi_owners_usernames = doi_results_map[doi]["doi_owners"]
        for ws_id in doi_results_map[doi]["ws_ids"]:
#            print("DOI: " + doi)
#            print("WS ID BEING USED:" + str(ws_id))
            ws_genomes_to_track = dict()
            ws_genomes_to_track = get_genomes_for_ws(db, ws_id)
#            print("WS_GENOMES_TO_TRACK: " + str(ws_genomes_to_track))
            parent_ws_id = None
            if ws_id in child_parent_ws_id_lookup:
                parent_ws_id = child_parent_ws_id_lookup[ws_id]

            all_copied_genomes_from_ws_list = grow_copied_list(copied_to_lookup_dict, [], ws_genomes_to_track)
#            print("ALL COPIED GENOMES LIST: " + str(all_copied_genomes_from_ws_list))
            for genome_copied in all_copied_genomes_from_ws_list:
                (temp_copied_object_ws_id, temp) = genome_copied.split("/",1)
                temp = "Do nothing with this"
                copied_object_ws_id = int(temp_copied_object_ws_id)
#                print("copied WS : " + str( copied_object_ws_id) + "  The copied owner lookup: " + str(ws_owners_lookup[copied_object_ws_id]))
                if ws_owners_lookup[copied_object_ws_id] not in doi_owners_usernames:
                    doi_results_map[doi]["ws_ids"][ws_id]["unique_users"].add(ws_owners_lookup[copied_object_ws_id])
                    doi_results_map[doi]["ws_ids"][ws_id]["unique_workspaces"].add(copied_object_ws_id)
                    if ws_id in child_parent_ws_id_lookup:
                        parent_ws_id = child_parent_ws_id_lookup[ws_id]
                        doi_results_map[doi]["ws_ids"][parent_ws_id]["unique_users"].add(ws_owners_lookup[copied_object_ws_id])
                        doi_results_map[doi]["ws_ids"][parent_ws_id]["unique_workspaces"].add(copied_object_ws_id)
#            print("RESULTS : " + str(doi_results_map[doi]["ws_ids"][ws_id]))
#    print()
#    print("Genomes to check")
#    print(str(ws_genomes_to_track))

#    print(str(child_parent_ws_id_lookup))
    return doi_results_map

def get_existing_unique_copied_workspaces(db_connection):
    """
    makes list of existing copied workspaces
    """
    publication_ws_copied_workspaces_map = dict()
    cursor = db_connection.cursor()
    get_publication_unique_workspaces_statement = ("select published_ws_id, copied_ws_id from metrics.publication_unique_workspaces;")
    cursor.execute(get_publication_unique_workspaces_statement)
    for row_values in cursor:
        published_ws  = row_values[0]
        copied_ws  = row_values[1]
        if published_ws not in publication_ws_copied_workspaces_map:
            publication_ws_copied_workspaces_map[published_ws] = list()
        publication_ws_copied_workspaces_map[published_ws].append(copied_ws)
    return publication_ws_copied_workspaces_map

def get_existing_unique_copied_usernames(db_connection):
    """
    makes list of existing usernames that copied data
    """
    publication_ws_copied_usernames_map = dict()
    cursor = db_connection.cursor()
    get_publication_unique_usernames_statement = ("select published_ws_id, copied_username from metrics.publication_unique_usernames;")
    cursor.execute(get_publication_unique_usernames_statement)
    for row_values in cursor:
        published_ws  = row_values[0]
        copied_username  = row_values[1]
        if published_ws not in publication_ws_copied_usernames_map:
            publication_ws_copied_usernames_map[published_ws] = list()
            publication_ws_copied_usernames_map[published_ws].append(copied_username)
    return publication_ws_copied_usernames_map

def upload_publications_data(db_connection,doi_results_map):
    """
    performs inserts into 3 tables : publication_metrics, publication_unique_usernames, publication_unique_workspaces
    """
    exitsting_workpsaces_lookup = get_existing_unique_copied_workspaces(db_connection)
    exitsting_usernames_lookup = get_existing_unique_copied_usernames(db_connection)

    pm_prep_cursor = db_connection.cursor(prepared=True)
    publication_metrics_insert_statement = (
        "insert into metrics.publication_metrics "
        "(ws_id, record_date, unique_users_count, unique_ws_ids_count) "
        "values(%s, now(), %s, %s);"
    )

    puw_prep_cursor = db_connection.cursor(prepared=True)
    publications_unique_workspaces_insert_statement = (
        "insert into metrics.publication_unique_workspaces "
        "(published_ws_id, copied_ws_id, first_seen_date) "
        "values( %s, %s, now()) ")

    puu_prep_cursor = db_connection.cursor(prepared=True)
    publications_unique_usernames_insert_statement = (
        "insert into metrics.publication_unique_usernames "
        "(published_ws_id, copied_username, first_seen_date) "
        "values( %s, %s, now()) ")

    for doi in doi_results_map:
        for ws_id in doi_results_map[doi]["ws_ids"]:
            unique_users_count = len(doi_results_map[doi]["ws_ids"][ws_id]["unique_users"])
            unique_workspaces_count = len(doi_results_map[doi]["ws_ids"][ws_id]["unique_workspaces"])
            pm_input = (ws_id, unique_users_count, unique_workspaces_count)
            pm_prep_cursor.execute(publication_metrics_insert_statement, pm_input)
            for copied_ws_id in doi_results_map[doi]["ws_ids"][ws_id]["unique_workspaces"]:
                needs_an_insert = False
                if ws_id not in exitsting_workpsaces_lookup:
                    needs_an_insert = True
                elif copied_ws_id not in exitsting_workpsaces_lookup[ws_id]:
                    needs_an_insert = True
                if needs_an_insert:
                    puw_input = (ws_id, copied_ws_id)
                    puw_prep_cursor.execute(publications_unique_workspaces_insert_statement, puw_input)
            for copied_username in doi_results_map[doi]["ws_ids"][ws_id]["unique_users"]:
                needs_an_insert = False
                if ws_id not in exitsting_usernames_lookup:
                    needs_an_insert = True
                elif copied_ws_id not in exitsting_usernames_lookup[ws_id]:
                    needs_an_insert = True
                if needs_an_insert:
                    puu_input = (ws_id, copied_username)
                    puu_prep_cursor.execute(publications_unique_usernames_insert_statement, puu_input)
    db_connection.commit()
    
def get_publication_metrics():
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
    query = "select DATE_FORMAT(max(record_date),'%Y-%m') from metrics.publication_metrics"
    cursor.execute(query)
    for db_date in cursor:
        db_date_month = db_date[0]
        if db_date_month == current_month:
            print(
                "THE PUBLICATION METRICS HAS BEEN RUN THIS MONTH. THE PROGRAM WILL EXIT"
            )
            exit()
        else:
            print("IT HAS NOT BEEN RUN THIS MONTH, WE WILL RUN THE PROGRAM")

    copied_to_lookup_dict = build_copy_lookup(db)
    ws_owners_lookup = get_workspace_owners(db)
                                                                            
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    doi_results_map = get_dois_and_narratives(cursor)
    doi_results_map = get_doi_owners_usernames(db, doi_results_map)
    doi_results_map = determine_publication_unique_users_and_ws_ids(db, doi_results_map, copied_to_lookup_dict, ws_owners_lookup)
    upload_publications_data(db_connection, doi_results_map)
    
#    print("FINAL MAP: " + str(doi_results_map))
            
get_publication_metrics()

    

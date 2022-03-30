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

# pp = pprint.PrettyPrinter(indent=4)

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

def get_static_narrative_counts():
    """
    returns a dict of ws_id to count of the nuber of static_workspaces made for it. 
    """
    service_wizard_url = os.environ["SERVICE_WIZARD_URL"]
    wiz = ServiceClient(service_wizard_url, use_url_lookup=True)
    stats = wiz.sync_call("StaticNarrative.list_static_narratives", [])
    static_narrative_counts = dict()
    for ws_id in stats[0]:
        static_narrative_counts[int(ws_id)] = len(stats[0][ws_id])
    return static_narrative_counts;

def get_workspaces(db, static_narrative_counts):
    """
    gets narrative workspaces information for non temporary workspaces
    """
    workspaces_dict = {}
    # Get all the legitimate workspaces and their respective user (saved(not_temp))
    workspaces_cursor = db.workspaces.find(
        {"moddate": {"$exists": True}},
        #  IF NOT DELETED NARRATIVES               {"del" : False, "moddate": {"$exists":True}},
        #  IF ONLY SAVED NARRATIVES                "meta" : {"k" : "is_temporary", "v" : "false"} },
        {"owner": 1, "ws": 1, "moddate": 1, "del": 1, "_id": 0},
    )
    for record in workspaces_cursor:
        if record["ws"] != 615:   ## and record['ws'] != 19217 :
            is_deleted_ws = 0
            if record["del"] == True:
                is_deleted_ws = 1
            workspaces_dict[record["ws"]] = {
                "ws_id": record["ws"],
                "username": record["owner"],
                "mod_date": record["moddate"],
                "initial_save_date": None,
                "top_lvl_object_count": 0,
                "total_object_count": 0,
                "visible_app_cells_count": 0,
                "code_cells_count": 0,
                "narrative_version": 0,
                "hidden_object_count": 0,
                "deleted_object_count": 0,
                "total_size": 0,
                "top_lvl_size": 0,
                "is_public": 0,
                "is_deleted": is_deleted_ws,
                "is_temporary": None,
                "number_of_shares": 0,
                "num_nar_obj_ids": 0,
                "static_narrative_count": 0,
                "unique_object_types_count": 0,
            }
            #See if it has static narratives, populate if it does.
            if record["ws"] in static_narrative_counts:
                workspaces_dict[record["ws"]]["static_narrative_count"] = static_narrative_counts[record["ws"]]

    workspace_is_temporary_cursor = db.workspaces.find(
        {
            "moddate": {"$exists": True},
            # {"del" : False,"moddate": {"$exists":True},
            "meta": {"k": "is_temporary", "v": "true"},
        },
        {"ws": 1, "_id": 0},
    )
    for record in workspace_is_temporary_cursor:
        workspaces_dict[record["ws"]]["is_temporary"] = 1

    workspace_not_temporary_cursor = db.workspaces.find(
        {
            "moddate": {"$exists": True},
            # {"del" : False, "moddate": {"$exists":True},
            "meta": {"k": "is_temporary", "v": "false"},
        },
        {"ws": 1, "_id": 0},
    )
    for record in workspace_not_temporary_cursor:
        workspaces_dict[record["ws"]]["is_temporary"] = 0

    return workspaces_dict


def get_workspace_shares(db, workspaces_dict):
    """
    gets number of shares per workspace
    """
    aggregation_string = [
        {"$match": {"perm": {"$in": [10, 20, 30]}}},
        {"$group": {"_id": "$id", "shared_count": {"$sum": 1}}},
    ]
    all_shared_perms_cursor = db.workspaceACLs.aggregate(aggregation_string)

    for record in db.workspaceACLs.aggregate(aggregation_string):
        if record["_id"] in workspaces_dict:
            workspaces_dict[record["_id"]]["number_of_shares"] = record["shared_count"]
    return workspaces_dict


def get_public_workspaces(db, workspaces_dict):
    """
    Gets IDs of public workspaces
    """
    public_workspaces_cursor = db.workspaceACLs.find({"user": "*"}, {"id": 1, "_id": 0})
    for record in public_workspaces_cursor:
        if record["id"] in workspaces_dict:
            workspaces_dict[record["id"]]["is_public"] = 1
    return workspaces_dict


""" OLD FOR APP CELL COUNTING NOW HANDLED BY NEW FUNCTION
def get_app_cell_count(wsadmin, narrative_ref, workspaces_with_app_cell_oddities):
#
    Gets the number of App Cells in the narrative
    See documentation for WS administer here 
    https://github.com/kbase/workspace_deluxe/blob/02217e4d63da8442d9eed6611aaa790f173de58e/docsource/administrationinterface.rst
#
    info = wsadmin.administer({'command': "getObjectInfo",
                               'params':  {"objects": [{"ref": narrative_ref}], "includeMetadata": 1}
                              })["infos"][0]
    meta = info[10]
    total_app_cells = 0
#    print("META: " + str(meta))
    for key in meta:
        if key.startswith("method."):
#            if(isinstance(meta[key], int)):
            try:
                total_app_cells += int(meta[key])
            except ValueError:
                workspaces_with_app_cell_oddities.add(narrative_ref)
                print("META: " + str(meta))
    return (total_app_cells, workspaces_with_app_cell_oddities)
"""


def get_narrative_cell_counts(wsadmin, narrative_ref):
    """
    Gets the number of App Cells and Code Cells in the narrative
    See documentation for WS administer here
    https://github.com/kbase/workspace_deluxe/blob/02217e4d63da8442d9eed6611aaa790f173de58e/docsource/administrationinterface.rst
    """
    app_count = 0
    code_count = 0
    is_suspect = 0
    try:
        nar_obj = wsadmin.administer(
            {"command": "getObjects", "params": {"objects": [{"ref": narrative_ref}]}}
        )["data"][0]["data"]

        if "cells" in nar_obj:
            for cell in nar_obj["cells"]:
                if cell["metadata"].get("kbase", {}).get("type") == "app":
                    app_count += 1
                elif cell["metadata"].get("kbase", {}).get("type") == "code":
                    code_count += 1
        return (app_count, code_count, is_suspect)
    except:
        # Shock node is suspect
        is_suspect = 1
        print("Narrative Ref : " + narrative_ref + " has a shock node issue")
        # try running on the previous version of the narrative if exists
        # if previous version does not exist return zeros
        (ws_id, obj_id, obj_ver) = narrative_ref.split("/")
        if int(obj_ver) > 1:
            new_ref = ws_id + "/" + obj_id + "/" + str(int(obj_ver) - 1)
            (app_count, code_count, dummy) = get_narrative_cell_counts(wsadmin, new_ref)
        return (app_count, code_count, is_suspect)


def get_kbase_staff(db_connection):
    """
    get list of KBase staff
    """
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    # get all existing users
    kbase_staff = set()
    query = "select username from metrics.user_info where kb_internal_user = 1;"
    cursor.execute(query)
    for username in cursor:
        kbase_staff.add(username[0])
    return kbase_staff


def get_objects(db, workspaces_dict, kbase_staff):
    """
    get object counts 
    as well as hidden del, size, is_narrative informationf for the workspace.
    """

    wsadmin = Workspace(ws_url, token=ws_user_token)
    # OBJECT COUNTS ACROSS ALL WORKSPACES.
    object_counts_dict = dict()
    # OBJECT COUNTS ACROSS USER WORKSPACES
    users_object_counts_dict = dict()
    # SAME FOR USER AND ALL
    #    {object_type_full : { "object_type" : ...,
    #                          "object_spec_version" : ...,
    #                          "last_mod_date" : max_date ,
    #                          "top_lvl_object_count" : # ,
    #                          "total_object_count" : # ,
    #                          "public_object_count" : # ,
    #                          "private_object_count" : # ,
    #                          "hidden_object_count" : # ,
    #                          "deleted_object_count" : # ,
    #                          "copy_count" : # ,
    #                          "total_size" : # ,
    #                          "top_lvl_size" : #}}

    #    workspaces_with_app_cell_oddities = set() #keeping track of workspaces with odd metadata where app cell counting fails
    narrative_refs_with_shock_node_issues = (
        set()
    )  # keep track of narrative refs with shocknode issues to keep track of later.

    ###########################
    # few debugging lines to do a smaller set of workspaces.
    ###########################
    #    ws_limit = 1 #For debugging purposes, if ws_limit is None then it does all workspaces.
    #    ws_counter = 0
    #    for ws_id in sorted(workspaces_dict.keys()):
    #        ws_counter += 1
    #        if ws_counter > ws_limit :
    #            del workspaces_dict[ws_id]
    ###########################
    # debugging lines to minimal Workspaces - these workspaces test different edge cases
    #temp_dict = dict()
    #    temp_dict[49114] = workspaces_dict[49114]
    #    temp_dict[3] = workspaces_dict[3]
    #    temp_dict[5009] = workspaces_dict[5009]
    #    temp_dict[1000] = workspaces_dict[1000]
    #    temp_dict[53462] = workspaces_dict[53462]
    #    temp_dict[324] = workspaces_dict[324]
    #    temp_dict[2777] = workspaces_dict[2777]
    #    temp_dict[6964] = workspaces_dict[6964]
    #    temp_dict[56261] = workspaces_dict[56261]
    #temp_dict[19216] = workspaces_dict[19216]
    #temp_dict[19217] = workspaces_dict[19217]   # THIS IS THE REFERENCE REFSEQ WS
    #temp_dict[19218] = workspaces_dict[19218]
    #temp_dict[52467] = workspaces_dict[52467]
    #temp_dict[52468] = workspaces_dict[52468]
    #temp_dict[13644] = workspaces_dict[13644]
    #temp_dict[30530] = workspaces_dict[30530]
    #temp_dict[46033] = workspaces_dict[46033]
    #temp_dict[46034] = workspaces_dict[46034]
    #workspaces_dict.clear()
    #workspaces_dict = temp_dict
    ###############
    for ws_id in sorted(workspaces_dict.keys()):
        min_save_date = None
        narr_obj_ids = set()
        print("PROCESSING WS : " + str(ws_id) + " : " + str(datetime.now()))
        is_narrative = False
        narrative_object_id = None
        unique_object_types = set()
        
        if workspaces_dict[ws_id]["is_deleted"] == 0:
            # DOES ANOTHER CHECK SINCE JOB RUNS SO LONG TO INSURE WS is still NOT DELETED
            workspaces_cursor = db.workspaces.find({"ws": ws_id}, {"del": 1, "_id": 0})
            for record in workspaces_cursor:
                if record["del"] == True:
                    workspaces_dict[ws_id]["is_deleted"] = 1
        #                print("IN DELETED WS CHECK : " + str(record["del"]))

        if workspaces_dict[ws_id]["is_deleted"] == 0:
            ws_info = wsadmin.administer(
                {"command": "getWorkspaceInfo", "params": {"id": str(ws_id)}}
            )
            ws_info_dict = ws_info[8]
            if "narrative" in ws_info_dict:
                narrative_object_id = ws_info_dict["narrative"]

        top_level_lookup_dict = dict()
        is_public_flag = workspaces_dict[ws_id]["is_public"]

        tl_ws_obj_cursor = db.workspaceObjects.find(
            {"ws": ws_id}, {"id": 1, "numver": 1, "del": 1, "hide": 1, "_id": 0}
        )
        for tl_object in tl_ws_obj_cursor:
            top_level_lookup_dict[tl_object["id"]] = {
                "numver": tl_object["numver"],
                "del": tl_object["del"],
                "hide": tl_object["hide"],
            }
        ws_obj_vers_cursor = db.workspaceObjVersions.find(
            {"ws": ws_id},
            {
                "type": 1,
                "id": 1,
                "ver": 1,
                "size": 1,
                "savedate": 1,
                "copied": 1,
                "_id": 0,
            },
        )
        for ws_obj_ver in ws_obj_vers_cursor:
            object_type_full = ws_obj_ver["type"]
            (object_type, object_spec_version) = object_type_full.split("-")
            if (object_tpe != "KBaseNarrative.Narrative" and object_type != "KBaseReport.Report"):
                unique_object_types.add(object_type)
            obj_id = ws_obj_ver["id"]
            if obj_id not in top_level_lookup_dict:
                #FOR Workspaces growing in size while processing the workspace
                continue
            obj_ver = ws_obj_ver["ver"]
            obj_size = ws_obj_ver["size"]
            top_obj_size = 0
            obj_save_date = ws_obj_ver["savedate"]
            obj_copied = 0
            if ws_obj_ver["copied"] is not None:
                obj_copied = 1
            is_hidden = 0
            is_deleted = 0
            is_top_level = 0

            workspaces_dict[ws_id]["total_object_count"] += 1
            workspaces_dict[ws_id]["total_size"] += obj_size

            if obj_ver == top_level_lookup_dict[obj_id]["numver"]:
                # means have maxed version of the object top_level
                is_top_level = 1
                top_obj_size = obj_size
                workspaces_dict[ws_id]["top_lvl_size"] += obj_size
                # IF A NARRATIVE OBJECT, UPDATE THE WS dict
                # INSURES IF MULTIPLE NARRATIVES IN WS U HAVE THE RIGHT NARRATIVE AS THE LIVE REAL ONE
                # print("narrative_object_id: " + str(narrative_object_id))
                # print("object_id: " + str(obj_id))
                #                if narrative_object_id is not None:
                #                    print("Equality Check: " + str(int(obj_id) == int(narrative_object_id)))
                try:
                    # Some narrative_object_id are not integer id, some are names, tests this
                    narrative_object_int = int(narrative_object_id)
                except:
                    narrative_object_int = None

                if (
                    narrative_object_id is not None
                    and narrative_object_int is not None
                    and int(obj_id) == int(narrative_object_id)
                ):
                    workspaces_dict[ws_id]["narrative_version"] = obj_ver
                    if top_level_lookup_dict[obj_id]["del"]:
                        workspaces_dict[ws_id]["visible_app_cells_count"] = 0
                        workspaces_dict[ws_id]["code_cells_count"] = 0
                    ##                    else:
                    ##                        (workspaces_dict[ws_id]["visible_app_cells_count"],workspaces_with_app_cell_oddities) = get_app_cell_count(wsadmin,
                    ##                                                                                                                      str(ws_id) + "/" + str(obj_id),
                    ##                                                                                                                      workspaces_with_app_cell_oddities)
                    else:
                        (
                            workspaces_dict[ws_id]["visible_app_cells_count"],
                            workspaces_dict[ws_id]["code_cells_count"],
                            is_suspect,
                        ) = get_narrative_cell_counts(
                            wsadmin, str(ws_id) + "/" + str(obj_id) + "/" + str(obj_ver)
                        )
                        if is_suspect == 1:
                            narrative_refs_with_shock_node_issues.add(
                                str(ws_id) + "/" + str(obj_id) + "/" + str(obj_ver)
                            )
                elif (
                    narrative_object_id is not None
                    and object_type == "KBaseNarrative.Narrative"
                ):
                    # THIS IS A SPECIAL CASE WHERE the narrative object id is narrative name not an ID like WS:2777
                    workspaces_dict[ws_id]["narrative_version"] = obj_ver
                    if top_level_lookup_dict[obj_id]["del"]:
                        workspaces_dict[ws_id]["visible_app_cells_count"] = 0
                        workspaces_dict[ws_id]["code_cells_count"] = 0
                    else:
                        (
                            workspaces_dict[ws_id]["visible_app_cells_count"],
                            workspaces_dict[ws_id]["code_cells_count"],
                            is_suspect,
                        ) = get_narrative_cell_counts(
                            wsadmin, str(ws_id) + "/" + str(obj_id) + "/" + str(obj_ver)
                        )
                        if is_suspect == 1:
                            narrative_refs_with_shock_node_issues.add(
                                str(ws_id) + "/" + str(obj_id) + "/" + str(obj_ver)
                            )
                elif object_type == "KBaseNarrative.Narrative":
                    # SPECIAL CASE THAT IS NOT THE ABOVE BUT THE NARRATIVE OBJECT DOES NOT HAVE META DATA
                    workspaces_dict[ws_id]["narrative_version"] = obj_ver
                # Get Workspace numbers
                workspaces_dict[ws_id]["top_lvl_object_count"] += 1
                if top_level_lookup_dict[obj_id]["hide"]:
                    workspaces_dict[ws_id]["hidden_object_count"] += obj_ver
                if top_level_lookup_dict[obj_id]["del"]:
                    workspaces_dict[ws_id]["deleted_object_count"] += obj_ver

            if object_type == "KBaseNarrative.Narrative":
                narr_obj_ids.add(obj_id)

            if obj_ver == 1:
                if min_save_date is None or obj_save_date < min_save_date:
                    min_save_date = obj_save_date

            if top_level_lookup_dict[obj_id]["hide"]:
                is_hidden = 1
            if top_level_lookup_dict[obj_id]["del"]:
                is_deleted = 1

            if object_type_full not in object_counts_dict:
                # First time seeing this object full type. Initialize in the dict
                object_counts_dict[object_type_full] = dict()
                object_counts_dict[object_type_full]["object_type"] = object_type
                object_counts_dict[object_type_full][
                    "object_spec_version"
                ] = object_spec_version
                object_counts_dict[object_type_full]["last_mod_date"] = obj_save_date
                object_counts_dict[object_type_full]["total_size"] = obj_size
                object_counts_dict[object_type_full]["top_lvl_size"] = top_obj_size
                object_counts_dict[object_type_full]["total_object_count"] = 1
                object_counts_dict[object_type_full]["copy_count"] = obj_copied
                object_counts_dict[object_type_full][
                    "top_lvl_object_count"
                ] = is_top_level
                object_counts_dict[object_type_full]["hidden_object_count"] = is_hidden
                object_counts_dict[object_type_full][
                    "deleted_object_count"
                ] = is_deleted
                object_counts_dict[object_type_full][
                    "public_object_count"
                ] = is_public_flag
                object_counts_dict[object_type_full]["max_object_size"] = obj_size
                if is_public_flag == 1:
                    object_counts_dict[object_type_full]["private_object_count"] = 0
                else:
                    object_counts_dict[object_type_full]["private_object_count"] = 1
            else:
                # Exists, add up values accordingly.
                # check if date is greater than last_mod_date
                standing_date = object_counts_dict[object_type_full]["last_mod_date"]
                if obj_save_date > standing_date:
                    object_counts_dict[object_type_full][
                        "last_mod_date"
                    ] = obj_save_date

                standing_max_object_size = object_counts_dict[object_type_full][
                    "max_object_size"
                ]
                if obj_size > standing_max_object_size:
                    object_counts_dict[object_type_full]["max_object_size"] = obj_size

                object_counts_dict[object_type_full]["total_size"] += obj_size
                object_counts_dict[object_type_full]["top_lvl_size"] += top_obj_size
                object_counts_dict[object_type_full]["total_object_count"] += 1
                object_counts_dict[object_type_full]["copy_count"] += obj_copied
                object_counts_dict[object_type_full][
                    "top_lvl_object_count"
                ] += is_top_level
                object_counts_dict[object_type_full]["hidden_object_count"] += is_hidden
                object_counts_dict[object_type_full][
                    "deleted_object_count"
                ] += is_deleted
                object_counts_dict[object_type_full][
                    "public_object_count"
                ] += is_public_flag
                if is_public_flag == 0:
                    object_counts_dict[object_type_full]["private_object_count"] += 1

            # SAME THING AS ABOVE BUT FOR USER ONLY WORKSPACE OBJECTS
            if workspaces_dict[ws_id]["username"] not in kbase_staff:
                if object_type_full not in users_object_counts_dict:
                    # First time seeing this object full type for users data. Initialize in the dict
                    users_object_counts_dict[object_type_full] = dict()
                    users_object_counts_dict[object_type_full][
                        "object_type"
                    ] = object_type
                    users_object_counts_dict[object_type_full][
                        "object_spec_version"
                    ] = object_spec_version
                    users_object_counts_dict[object_type_full][
                        "last_mod_date"
                    ] = obj_save_date
                    users_object_counts_dict[object_type_full]["total_size"] = obj_size
                    users_object_counts_dict[object_type_full][
                        "top_lvl_size"
                    ] = top_obj_size
                    users_object_counts_dict[object_type_full]["total_object_count"] = 1
                    users_object_counts_dict[object_type_full][
                        "copy_count"
                    ] = obj_copied
                    users_object_counts_dict[object_type_full][
                        "top_lvl_object_count"
                    ] = is_top_level
                    users_object_counts_dict[object_type_full][
                        "hidden_object_count"
                    ] = is_hidden
                    users_object_counts_dict[object_type_full][
                        "deleted_object_count"
                    ] = is_deleted
                    users_object_counts_dict[object_type_full][
                        "public_object_count"
                    ] = is_public_flag
                    users_object_counts_dict[object_type_full][
                        "max_object_size"
                    ] = obj_size
                    if is_public_flag == 1:
                        users_object_counts_dict[object_type_full][
                            "private_object_count"
                        ] = 0
                    else:
                        users_object_counts_dict[object_type_full][
                            "private_object_count"
                        ] = 1
                else:
                    # Exists, add up values accordingly.
                    # check if date is greater than last_mod_date
                    standing_date = users_object_counts_dict[object_type_full][
                        "last_mod_date"
                    ]
                    if obj_save_date > standing_date:
                        users_object_counts_dict[object_type_full][
                            "last_mod_date"
                        ] = obj_save_date

                    standing_max_object_size = users_object_counts_dict[
                        object_type_full
                    ]["max_object_size"]
                    if obj_size > standing_max_object_size:
                        users_object_counts_dict[object_type_full][
                            "max_object_size"
                        ] = obj_size

                    users_object_counts_dict[object_type_full]["total_size"] += obj_size
                    users_object_counts_dict[object_type_full][
                        "top_lvl_size"
                    ] += top_obj_size
                    users_object_counts_dict[object_type_full][
                        "total_object_count"
                    ] += 1
                    users_object_counts_dict[object_type_full][
                        "copy_count"
                    ] += obj_copied
                    users_object_counts_dict[object_type_full][
                        "top_lvl_object_count"
                    ] += is_top_level
                    users_object_counts_dict[object_type_full][
                        "hidden_object_count"
                    ] += is_hidden
                    users_object_counts_dict[object_type_full][
                        "deleted_object_count"
                    ] += is_deleted
                    users_object_counts_dict[object_type_full][
                        "public_object_count"
                    ] += is_public_flag
                    if is_public_flag == 0:
                        users_object_counts_dict[object_type_full][
                            "private_object_count"
                        ] += 1
        workspaces_dict[ws_id]["num_nar_obj_ids"] = len(narr_obj_ids)
        workspaces_dict[ws_id]["unique_object_types_count"] = len(unique_object_types)
        workspaces_dict[ws_id]["initial_save_date"] = min_save_date
    return (
        workspaces_dict,
        object_counts_dict,
        users_object_counts_dict,
        narrative_refs_with_shock_node_issues,
    )


def upload_workspace_stats():
    """
    Is the "main" function to get and upload both workspace data as well as workspace object summary stats
    """
    start_time = time.time()

    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
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
    query = "select DATE_FORMAT(max(record_date),'%Y-%m') from metrics.users_workspace_object_counts"
    cursor.execute(query)
    for db_date in cursor:
        db_date_month = db_date[0]

    if db_date_month == current_month:
        print(
            "THE WORKSPACE and WS OBJECTS COUNTS UPLOADER HAS BEEN RUN THIS MONTH. THE PROGRAM WILL EXIT"
        )
        exit()
    else:
        print("IT HAS NOT BEEN RUN THIS MONTH, WE WILL RUN THE PROGRAM")

    client = MongoClient(mongoDB_metrics_connection + to_workspace)
    db = client.workspace

    static_narrative_counts = get_static_narrative_counts()
    workspaces_dict = get_workspaces(db, static_narrative_counts)
    get_ws_top_info_time = time.time() - start_time
    kbase_staff = get_kbase_staff(db_connection)
    db_connection.close()
    workspaces_dict = get_workspace_shares(db, workspaces_dict)
    workspaces_dict = get_public_workspaces(db, workspaces_dict)
    # THIS NEXT STEP TAKES A LONG TIME. MYSQL CONNECTION WILL TIME OUT)
    (
        workspaces_dict,
        object_counts_dict,
        users_object_counts_dict,
        narrative_refs_with_shock_node_issues,
    ) = get_objects(db, workspaces_dict, kbase_staff)
    print("TOTAL WS Number : " + str(len(workspaces_dict)))
    gather_time = time.time() - start_time

    # connect to mysql AGAIN
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )

    #    print("--- gather data %s seconds ---" % (gather_time))
    #    print("WORKSPACE DICT : ")
    #    pp.pprint(workspaces_dict)
    #    print("OBJECT COUNTS DICT : ")
    #    pp.pprint(object_counts_dict)

    # WORKSPACES UPLOADING
    prep_cursor = db_connection.cursor(prepared=True)
    workspaces_insert_statement = (
        "insert into metrics.workspaces "
        "(ws_id, username, mod_date, initial_save_date, record_date, "
        "top_lvl_object_count, total_object_count, "
        "visible_app_cells_count, code_cells_count, narrative_version, "
        "hidden_object_count, deleted_object_count, "
        "total_size, top_lvl_size, is_public, "
        "is_temporary, is_deleted, number_of_shares, "
        "num_nar_obj_ids, static_narratives_count,unique_object_types_count) "
        "values(%s,%s, %s, %s, now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    )

    for ws_id in sorted(workspaces_dict.keys()):
        input = (
            ws_id,
            workspaces_dict[ws_id]["username"],
            workspaces_dict[ws_id]["mod_date"],
            workspaces_dict[ws_id]["initial_save_date"],
            workspaces_dict[ws_id]["top_lvl_object_count"],
            workspaces_dict[ws_id]["total_object_count"],
            workspaces_dict[ws_id]["visible_app_cells_count"],
            workspaces_dict[ws_id]["code_cells_count"],
            workspaces_dict[ws_id]["narrative_version"],
            workspaces_dict[ws_id]["hidden_object_count"],
            workspaces_dict[ws_id]["deleted_object_count"],
            workspaces_dict[ws_id]["total_size"],
            workspaces_dict[ws_id]["top_lvl_size"],
            workspaces_dict[ws_id]["is_public"],
            workspaces_dict[ws_id]["is_temporary"],
            workspaces_dict[ws_id]["is_deleted"],
            workspaces_dict[ws_id]["number_of_shares"],
            workspaces_dict[ws_id]["num_nar_obj_ids"],
            workspaces_dict[ws_id]["static_narrative_count"],
            workspaces_dict[ws_id]["unique_object_types_count"],
        )
        prep_cursor.execute(workspaces_insert_statement, input)

    print("TOTAL WS Number : " + str(len(workspaces_dict)))
    print("--- get ws top info time took %s seconds ---" % (get_ws_top_info_time))
    print("--- gather data %s seconds ---" % (gather_time))

    workspace_time = time.time() - (gather_time + start_time)
    print("--- workspaces uploading  %s seconds ---" % (workspace_time))

    # OBJECT COUNTS UPLOADING
    prep_cursor_obj = db_connection.cursor(prepared=True)
    obj_counts_insert_statement = (
        "insert into metrics.workspace_object_counts "
        "(object_type, object_spec_version, object_type_full, "
        "record_date, last_mod_date, top_lvl_object_count, "
        "total_object_count, public_object_count, private_object_count, "
        "hidden_object_count, deleted_object_count, copy_count, "
        "total_size, top_lvl_size, max_object_size) "
        "values(%s,%s, %s, now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    )
    # print("OBJECT COUNTS DICT : " + str(object_counts_dict))
    for obj_full in sorted(object_counts_dict):
        obj_input = (
            object_counts_dict[obj_full]["object_type"],
            object_counts_dict[obj_full]["object_spec_version"],
            obj_full,
            object_counts_dict[obj_full]["last_mod_date"],
            object_counts_dict[obj_full]["top_lvl_object_count"],
            object_counts_dict[obj_full]["total_object_count"],
            object_counts_dict[obj_full]["public_object_count"],
            object_counts_dict[obj_full]["private_object_count"],
            object_counts_dict[obj_full]["hidden_object_count"],
            object_counts_dict[obj_full]["deleted_object_count"],
            object_counts_dict[obj_full]["copy_count"],
            object_counts_dict[obj_full]["total_size"],
            object_counts_dict[obj_full]["top_lvl_size"],
            object_counts_dict[obj_full]["max_object_size"],
        )
        prep_cursor.execute(obj_counts_insert_statement, obj_input)

    object_time = time.time() - (workspace_time + gather_time + start_time)
    print("--- object counts uploading %s seconds ---" % (object_time))

    # USERS_OBJECT COUNTS UPLOADING
    prep_cursor_obj = db_connection.cursor(prepared=True)
    users_obj_counts_insert_statement = (
        "insert into metrics.users_workspace_object_counts "
        "(object_type, object_spec_version, object_type_full, "
        "record_date, last_mod_date, top_lvl_object_count, "
        "total_object_count, public_object_count, private_object_count, "
        "hidden_object_count, deleted_object_count, copy_count, "
        "total_size, top_lvl_size, max_object_size) "
        "values(%s,%s, %s, now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    )
    for obj_full in sorted(users_object_counts_dict):
        users_obj_input = (
            users_object_counts_dict[obj_full]["object_type"],
            users_object_counts_dict[obj_full]["object_spec_version"],
            obj_full,
            users_object_counts_dict[obj_full]["last_mod_date"],
            users_object_counts_dict[obj_full]["top_lvl_object_count"],
            users_object_counts_dict[obj_full]["total_object_count"],
            users_object_counts_dict[obj_full]["public_object_count"],
            users_object_counts_dict[obj_full]["private_object_count"],
            users_object_counts_dict[obj_full]["hidden_object_count"],
            users_object_counts_dict[obj_full]["deleted_object_count"],
            users_object_counts_dict[obj_full]["copy_count"],
            users_object_counts_dict[obj_full]["total_size"],
            users_object_counts_dict[obj_full]["top_lvl_size"],
            users_object_counts_dict[obj_full]["max_object_size"],
        )
        prep_cursor.execute(users_obj_counts_insert_statement, users_obj_input)

    users_object_time = time.time() - (
        workspace_time + gather_time + start_time + object_time
    )
    print("--- users object counts uploading %s seconds ---" % (users_object_time))

    # SUSPECT SHOCK NODE INSERTING/UPDATING
    # If exists, update the last_seen_date
    # If does not exist make a new record.
    # query to see if it exists
    prep_cursor_ssn_exists = db_connection.cursor(prepared=True)
    ssn_exists_query = (
        "select count(*) from metrics.suspect_shock_nodes "
        "where ws_obj_reference = %s and shock_node = %s"
    )

    # query_to_update suspect_shock_nodes
    prep_cursor_ssn_update = db_connection.cursor(prepared=True)
    suspect_shock_node_update_statement = (
        "update metrics.suspect_shock_nodes "
        "set last_seen_date = now() "
        "where ws_obj_reference = %s and shock_node = %s;"
    )

    # query_to_insert suspect_shock_nodes
    prep_cursor_ssn_insert = db_connection.cursor(prepared=True)
    suspect_shock_node_insert_statement = (
        "insert into metrics.suspect_shock_nodes "
        "(ws_obj_reference, shock_node, "
        "first_seen_date, last_seen_date, ws_id) "
        "values(%s,%s, now(), now(), %s);"
    )

    #    print("narrative_refs_with_shock_node_issues : " + str(narrative_refs_with_shock_node_issues))
    for nar_ref in narrative_refs_with_shock_node_issues:
        #        print("IN SHOCK NODES ISSUES : " + nar_ref)
        (ws_id, nar_id, nar_version) = nar_ref.split("/")
        chksum_cursor = db.workspaceObjVersions.find(
            {"ws": int(ws_id), "id": int(nar_id), "ver": int(nar_version)},
            {"chksum": 1, "_id": 0},
        )
        chksum = None
        for chksum_arr in chksum_cursor:
            chksum = chksum_arr["chksum"]
        node_cursor = db.shock_nodeMap.find({"chksum": chksum}, {"node": 1, "_id": 0})
        node = None
        for node_arr in node_cursor:
            node = node_arr["node"]

        # check if nar_ref_node combo exists:
        prep_cursor_ssn_exists.execute(ssn_exists_query, (nar_ref, node))
        exists_count_arr = prep_cursor_ssn_exists.fetchone()
        if node:
            if exists_count_arr[0] == 0:
                # means a new problem narrative/node needs an insert
                prep_cursor_ssn_insert.execute(
                    suspect_shock_node_insert_statement, (nar_ref, node, int(ws_id))
                )
            else:
                # means the nar_ref node existed . Update the last seen date.
                prep_cursor_ssn_insert.execute(
                    suspect_shock_node_update_statement, (nar_ref, node)
                )
        else:
            print("The object id " +
                  str(nar_ref) +
                  "has a suspect shock node behavior, but has no node")

    db_connection.commit()


# upload_workspace_stats()

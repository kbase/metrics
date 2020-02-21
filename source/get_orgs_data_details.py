from pymongo import MongoClient
from pymongo import ReadPreference
import os
import mysql.connector as mysql

import datetime

metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']
mongoDB_metrics_connection = os.environ['MONGO_PATH']

sql_host = os.environ['SQL_HOST']
query_on = os.environ['QUERY_ON']
to_groups =  os.environ['GRP_SUFFIX']

def get_workspaces(db_connection):
    """
    gets user narrative workspaces - capturing ws_id, owner, creation_date, last_mod_date, is_deleted, is_public. 
    """
    workspaces_dict = {}

    cursor = db_connection.cursor()
    query = "use "+query_on
    cursor.execute(query)

    query = "select ws_id, ws.username as username, initial_save_date, mod_date, "\
            "is_deleted, is_public, top_lvl_object_count, total_object_count "\
            "from metrics_reporting.workspaces_current ws "\
            "inner join metrics.user_info ui on ws.username = ui.username "\
            "where ws.narrative_version > 0;"
    cursor.execute(query)
    for (record) in cursor:
        workspaces_dict[record[0]] = {"ws_id" : record[0],
                                      "username" : record[1],
                                      "creation_date" : record[2],
                                      "mod_date" : record[3],
                                      "is_deleted" : record[4],
                                      "is_public" : record[5],
                                      "top_lvl_object_count" : record[6],
                                      "total_object_count" : record[7]}
    return workspaces_dict


def get_orgs(workspaces_dict):
    """
    gets orgs
    """
    client= MongoClient(mongoDB_metrics_connection+to_groups)
    db = client.groups


    orgs_dict = dict()
    #  "id" -> {"name"->val,
    #           "create" -> val,
    #           "mod" -> val,
    #           "members" -> [{"user"->val,
    #                        "join" ->val},..]
    #           "narratives" -> [{"ws_id" -> val,
    #                             "username" -> val,
    #                             "createtion_date" -> val,
    #                             "mod_date" -> val,
    #                             "is_public" -> val,
    #                             "is_deleted" -> val,
    #                             "top_lvl_object_count" -> val
    #                             "total_object_count" -> val,
    #                             "added_date -> val},....]
    
    #only gives orgs with at least 5 shared narratives
#    orgs_query = db.groups.find({"id":"enigma","resources.workspace": {"$exists":True}, "$where":"this.resources.workspace.length > 4"},
    orgs_query = db.groups.find({"resources.workspace": {"$exists":True}, "$where":"this.resources.workspace.length > 4"},
	                        {"id":1,"name":1, "create":1, "mod":1, "memb":1, "resources.workspace":1,"_id":0})
    for record in orgs_query:
        orgs_dict[record["id"]] = {"name" : record["name"],
                                   "create" : record["create"],
                                   "mod" : record["mod"],
                                   "members" : list(),
                                   "narratives" : list()
        }

        members = record["memb"]
        for member in members:
#            print("MEMBER : " + str(member))
            orgs_dict[record["id"]]["members"].append({"user":member["user"],
                                                       "join":member["join"]})

        workspaces = record["resources"]["workspace"]
#        print("WORKSPACES: " + str(workspaces))
        earliest_date = datetime.datetime.utcnow()

        for workspace in workspaces:
            ws_id = int(workspace["aid"])
#            print("WSID: " + str(ws_id))
            if ws_id in workspaces_dict:
                temp_ws_dict = workspaces_dict[ws_id]
            else:
                temp_ws_dict = {"ws_id" : ws_id}
            if "add" in workspace:
#                print("TYPE OF OBJECT : " + str(type(workspace["add"])))
#                print("TYPE OF OBJECT EARLIEST DATE: " + str(type(earliest_date)))
                if workspace["add"] < earliest_date:
                    earliest_date = workspace["add"]
                temp_ws_dict["date_added_to_org"] = workspace["add"]
            orgs_dict[record["id"]]["narratives"].append(temp_ws_dict)

        for narrative in orgs_dict[record["id"]]["narratives"]:
            if "date_added_to_org" not in narrative:
                narrative["date_added_to_org"] = earliest_date 

#    print("WORKSPACES DICT LENGTH : " + str(len(workspaces_dict)))
#    print("WORKSPACES 15184 : " + str(workspaces_dict[15184]))
    return (orgs_dict)


def get_orgs_details():
    """
    get the ws sharing details
    """

    #connect to mysql
    db_connection = mysql.connect(
        host = sql_host,
        user = "metrics",
        passwd = metrics_mysql_password,
        database = "metrics"
    )

    cursor = db_connection.cursor()
    query = "use "+query_on
    cursor.execute(query)
    
    workspaces_dict = get_workspaces(db_connection)
    orgs_dict = get_orgs(workspaces_dict)

    #print out ORGS top level information
    print("ORGS TOP LEVEL INFORMATION")
    print("ORG ID\tORG Name\tORG Creation Date\tORG Last Modified Date")
    for org_id in orgs_dict:
        print("{}\t{}\t{}\t{}".format(org_id,orgs_dict[org_id]["name"],orgs_dict[org_id]["create"],orgs_dict[org_id]["mod"])) 
    print()

    #print out ORGS members information
    print("ORGS MEMBER INFORMATION")
    print("ORG ID\tMember Name\tJoin Date")
    for org_id in orgs_dict:              
        for member in orgs_dict[org_id]["members"]:
            print("{}\t{}\t{}".format(org_id, member["user"], member["join"])) 
    print()

    #print out ORGS narratives information
    print("ORGS NARRATIVES INFORMATION")
    print("ORG ID\tWS_ID\tOwner\tCreation Date\tMod Date\tDate_Added_To_ORG\tIS_Public\tIS_Deleted\ttop_lvl_object_count\ttotal_object_count")
    for org_id in orgs_dict:
        for narrative in orgs_dict[org_id]["narratives"]:
            if "username" in narrative:
                print("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(org_id, narrative["ws_id"],narrative["username"],
                                                                      narrative["creation_date"],narrative["mod_date"],
                                                                      narrative["date_added_to_org"],narrative["is_public"],
                                                                      narrative["is_deleted"],narrative["top_lvl_object_count"],
                                                                      narrative["total_object_count"]))
            else:
                print("{}\t{}\t\t\t\t{}".format(org_id, narrative["ws_id"], narrative["date_added_to_org"]))
    print()

get_orgs_details()

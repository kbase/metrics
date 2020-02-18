from pymongo import MongoClient
from pymongo import ReadPreference
import os
import mysql.connector as mysql

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
            "where ui.kb_internal_user = 0 and ws.number_of_shares > 0 "\
            "and narrative_version > 0;"
    cursor.execute(query)
    for (record) in cursor:
        workspaces_dict[record[0]] = {"username" : record[1],
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
    #                             "org_date" -> val,
    #                             "username" -> val,
    #                             "created" -> val,
    #                             "mod" -> val,
    #                             "is_public" -> val,
    #                             "is_deleted" -> val,
    #                             "top_lvl_object_count" -> val
    #                             "total_object_count" -> val},....]
    
    #only gives orgs with at least 5 shared narratives
    orgs_query = db.groups.find({"id":"enigma","resources.workspace": {"$exists":True}, "$where":"this.resources.workspace.length > 4"},
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
            print("MEMBER : " + str(member))
            orgs_dict[record["id"]]["members"].append({"user":member["user"],
                                                       "join":member["join"]})
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

    print("ORG DETAILS : " + str(orgs_dict))
"""                                                      
    ################
    # Print the header line:
    ################
    header_line = "Narrative ID\tOwner\tCreation Date\tLast Modified\tis_deleted\tis_public"
    for i in range(max_shared_count):
        header_line += "\tShared_person_{}\tShare_Type_{}\tis_KB_Staff_{}".format(str(i+1),str(i+1),str(i+1))
    print(header_line)

    ###############
    # Print the WS rows
    ###############
    for ws_id in workspaces_dict:
        print("{}\t{}\t{}\t{}\t{}\t{}\t{}".format(str(ws_id),
                                                  workspaces_dict[ws_id]["username"],
                                                  workspaces_dict[ws_id]["creation_date"],
                                                  workspaces_dict[ws_id]["mod_date"],
                                                  str(workspaces_dict[ws_id]["is_deleted"]),
                                                  str(workspaces_dict[ws_id]["is_public"]),
                                                  "\t".join(workspaces_dict[ws_id]["shares_list"])))
"""    
get_orgs_details()

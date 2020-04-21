from pymongo import MongoClient
from pymongo import ReadPreference
import json as _json
import os
import mysql.connector as mysql
from pprint import pprint

"""
THIS SCRIPT WAS USED TO DETERMINE THE ISSUE WITH OLD WAY QZ DID NARRATIVE COUNTING.
Before only the first version of an object was looked for in the workspaceObjects Mongo collection.
The problem is that collection only looks at the top level (highest version number) of that object.
So as a result there were two problems, workspaces that had no first version of objects as their top level
object would be excluded. 
The second is the timestamp was arbitrary as far as a first version of an object and would
change in future runnings if that first version of the object got a nother version.
I used this to confirm the discrepancy I saw between her approach and my (Jason B)
approach, The workspaceObjVersion table is the correct place to look
"""
# NOTE get_user_info_from_auth2 sets up the initial dict.
# The following functions update certain fields in the dict.
# So get_user_info_from_auth2 must be called before get_internal_users and get_user_orgs_count

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
mongoDB_metrics_connection = os.environ["MONGO_PATH"]

sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]

to_workspace = os.environ["WRK_SUFFIX"]


def get_non_kbase_staff():
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    # get all existing users
    non_kb_staff = set()
    query = "select username from metrics.user_info where kb_internal_user = 0;"
    cursor.execute(query)
    for username in cursor:
        non_kb_staff.add(username[0])
    print(str(non_kb_staff))
    return non_kb_staff


def get_narratives_first_save():
    """ get the first save all narratives and put into monthly buckets """

    client_workspace = MongoClient(mongoDB_metrics_connection + to_workspace)
    db_workspace = client_workspace.workspace

    non_kb_staff = get_non_kbase_staff()
    legitimate_narratives_set = set()
    workspace_nar_count = dict()
    narratives_with_ver1_set = set()
    all_narratives_set = set()

    # Get all the legitimate narratives and and their respective user (not del, saved(not_temp))
    all_nar_cursor = db_workspace.workspaces.find(
        {"del": False, "meta": {"k": "is_temporary", "v": "false"}},
        {"ws": 1, "owner": 1, "_id": 0},
    )
    for record in all_nar_cursor:
        if record["owner"] in non_kb_staff:
            legitimate_narratives_set.add(record["ws"])

    ws_objects_cur = db_workspace.workspaceObjects.find(
        {}, {"ws": 1, "numver": 1, "_id": 0}
    )
    for record in ws_objects_cur:
        if record["ws"] in legitimate_narratives_set:
            all_narratives_set.add(record["ws"])
            if record["numver"] == 1:
                narratives_with_ver1_set.add(record["ws"])

    narratives_without_ver1_set = all_narratives_set - narratives_with_ver1_set

    print("all_narratives_set : " + str(len(all_narratives_set)))
    print("narratives_with_ver1_set : " + str(len(narratives_with_ver1_set)))
    print("narratives_without_ver1_set : " + str(len(narratives_without_ver1_set)))


get_narratives_first_save()

from pymongo import MongoClient
from pymongo import ReadPreference
import os
import mysql.connector as mysql

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
mongoDB_metrics_connection = os.environ["MONGO_PATH"]

sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]
to_workspace = os.environ["WRK_SUFFIX"]

"""
THIS IS A SPECIAL SCRIPT TO GIVE ADAM INFORMATION TO CREATE A USER SHARING CONNECTION GRAPH
Step	What to Do
1	Git clone https://github.com/kbase/metrics
2	cd metrics
3	configure the .env file (ask Jason or Steve for details)
4	Build  from metrics directory: docker build . -t test_build
5	docker-compose run --rm metrics ../bin/custom_scripts/get_workspace_share_details.sh > users_sharing.txt
6	scp that file to your computer and use accordingly.
	example from your computer run:
	scp jkbaumohl@login1.berkeley.kbase.us:/homes/oakland/jkbaumohl/metrics/users_sharing.txt ./Desktop/

Creates output like this:
Narrative ID	Owner	Creation Date	Last Modified	is_deleted	is_public	Shared_person_0	Share_Type_0	is_KB_Staff_0	Shared_person_1	Share_Type_1	is_KB_Staff_1	Shared_person_2	Share_Type_2	is_KB_Staff_2	...    Shared_person_31	Share_Type_31	is_KB_Staff_31
24863	aafoutouhi	2017-09-21	2017-09-22	0	0	weimer	edit	0
24900	aafoutouhi	2017-09-22	2017-09-22	0	0	weimer	edit	0
34151	aappaakbase	2018-07-08	2018-07-09	0	0	saysonsg	view	0
"""


def get_workspaces(db_connection):
    """
    gets user narrative workspaces - capturing ws_id, owner, creation_date, last_mod_date, is_deleted, is_public. 
    """
    workspaces_dict = {}

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    query = (
        "select ws_id, ws.username as username, initial_save_date, mod_date, "
        "is_deleted, is_public "
        "from metrics_reporting.workspaces_current ws "
        "inner join metrics.user_info ui on ws.username = ui.username "
        "where ui.kb_internal_user = 0 and ws.number_of_shares > 0 "
        "and narrative_version > 0;"
    )
    cursor.execute(query)
    for record in cursor:
        workspaces_dict[record[0]] = {
            "username": record[1],
            "creation_date": record[2],
            "mod_date": record[3],
            "is_deleted": record[4],
            "is_public": record[5],
            "shares_list": list(),
        }
    return workspaces_dict


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


def get_workspace_shares(workspaces_dict, kb_staff):
    """
    gets shares per workspace
    """
    client = MongoClient(mongoDB_metrics_connection + to_workspace)
    db = client.workspace
    max_shared_count = 0
    perms_dict = {10: "view", 20: "edit", 30: "admin"}

    shares_query = db.workspaceACLs.find(
        {"perm": {"$in": [10, 20, 30]}}, {"id": 1, "user": 1, "perm": 1, "_id": 0}
    )
    for record in shares_query:
        if record["id"] in workspaces_dict:
            # do stuff as it is a users narrative and has at least 1 share.
            is_kb_staff = 0
            if record["user"] in kb_staff:
                is_kb_staff = 1
            share_entry = [record["user"], perms_dict[record["perm"]], str(is_kb_staff)]
            workspaces_dict[record["id"]]["shares_list"].extend(share_entry)

    max_shared_count = 0
    for ws in workspaces_dict:
        share_number = len(workspaces_dict[ws]["shares_list"])
        if share_number > max_shared_count:
            max_shared_count = share_number
    return (workspaces_dict, int(max_shared_count / 3))


def get_workspace_share_details():
    """
    get the ws sharing details
    """

    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    workspaces_dict = get_workspaces(db_connection)
    kb_staff = get_kbase_staff(db_connection)
    (workspaces_dict, max_shared_count) = get_workspace_shares(
        workspaces_dict, kb_staff
    )

    ################
    # Print the header line:
    ################
    header_line = (
        "Narrative ID\tOwner\tCreation Date\tLast Modified\tis_deleted\tis_public"
    )
    for i in range(max_shared_count):
        header_line += "\tShared_person_{}\tShare_Type_{}\tis_KB_Staff_{}".format(
            str(i + 1), str(i + 1), str(i + 1)
        )
    print(header_line)

    ###############
    # Print the WS rows
    ###############
    for ws_id in workspaces_dict:
        print(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}".format(
                str(ws_id),
                workspaces_dict[ws_id]["username"],
                workspaces_dict[ws_id]["creation_date"],
                workspaces_dict[ws_id]["mod_date"],
                str(workspaces_dict[ws_id]["is_deleted"]),
                str(workspaces_dict[ws_id]["is_public"]),
                "\t".join(workspaces_dict[ws_id]["shares_list"]),
            )
        )


get_workspace_share_details()

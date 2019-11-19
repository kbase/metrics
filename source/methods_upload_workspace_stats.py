from pymongo import MongoClient
from pymongo import ReadPreference
import json as _json
import os
import mysql.connector as mysql
import requests
requests.packages.urllib3.disable_warnings()

# NOTE get_user_info_from_auth2 sets up the initial dict. 
#The following functions update certain fields in the dict.
# So get_user_info_from_auth2 must be called before get_internal_users and get_user_orgs_count

metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']
mongoDB_metrics_connection = os.environ['MONGO_PATH']

sql_host = os.environ['SQL_HOST']
query_on = os.environ['QUERY_ON']

to_workspace =  os.environ['WRK_SUFFIX']

def get_workspaces(db):
    """
    gets narrative workspaces information for non temporary workspaces
    """
    workspaces_dict = {}
    #Get all the legitimate narratives and and their respective user (saved(not_temp))
    workspaces_cursor = db.workspaces.find({"del" : False, "moddate": {"$exists":True}},
#                                            "meta" : {"k" : "is_temporary", "v" : "false"} },
                                           {"owner":1,"ws":1,"moddate":1,"_id":0})
    for record in workspaces_cursor:
        workspaces_dict[record["ws"]] = {"ws_id" : record["ws"],
                                         "username" : record["owner"],
                                         "mod_date" : record["moddate"],
                                         "number_of_shares" : 0,
                                         "is_public" : 0
        }
    return workspaces_dict

def get_workspace_shares(db, workspaces_dict):
    """
    gets number of shares per workspace
    """
    aggregation_string=[
        {"$match" : {"perm" : { "$in": [ 10,20,30 ]}}},
        {"$group" : {"_id" : "$id", "shared_count" : { "$sum" : 1 }}}]
    all_shared_perms_cursor=db.workspaceACLs.aggregate(aggregation_string)

    for record in db.workspaceACLs.aggregate(aggregation_string):
        if record["_id"] in workspaces_dict:
            workspaces_dict[record["_id"]]["number_of_shares"] = record["shared_count"]
    return workspaces_dict

def get_public_workspaces(db, workspaces_dict):
    """
    Gets IDs of public workspaces
    """
    public_workspaces_cursor = db.workspaceACLs.find({"user" : "*"},
                                                     {"id":1,"_id":0})
    for record in public_workspaces_cursor:
        if record["id"] in workspaces_dict:
            workspaces_dict[record["id"]]["is_public"] = 1
    return workspaces_dict

def upload_workspace_stats():
    """
    Is the "main" function to get and upload both workspace data as well as workspace object summary stats
    """
    client= MongoClient(mongoDB_metrics_connection+to_workspace)
    db = client.workspace

    workspaces_dict = get_workspaces(db)
    workspaces_dict =get_workspace_shares(db,workspaces_dict)
    print("workspaces_dict : " + str(workspaces_dict))
    print("TOTAL WS Number : " + str(len(workspaces_dict)))
    print("WS 49114 : " + str(workspaces_dict[49114]))
    print("WS 3 : " + str(workspaces_dict[3]))

def old_upload_function():
    total_users = len(user_stats_dict.keys())
    rows_info_inserted = 0;
    rows_info_updated = 0;
    rows_stats_inserted = 0;
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

    #get all existing users    
    existing_user_info = dict()
    query = "select username, display_name, email, orcid, kb_internal_user, institution, " \
            "country, signup_date, last_signin_date from user_info"
    cursor.execute(query)
    for (username, display_name, email, orcid, kb_internal_user, institution, 
         country, signup_date, last_signin_date) in cursor:
        existing_user_info[username]={"name":display_name,
                                       "email":email, 
                                       "orcid":orcid,
                                       "kb_internal_user":kb_internal_user,
                                       "institution":institution,
                                       "country":country, 
                                       "signup_date":signup_date,
                                       "last_signin_date":last_signin_date}

    print("Number of existing users:" + str(len(existing_user_info)))

    prep_cursor = db_connection.cursor(prepared=True)
    user_info_insert_statement = "insert into user_info " \
                                 "(username,display_name,email,orcid,kb_internal_user, " \
                                 "institution,country,signup_date,last_signin_date) " \
                                 "values(%s,%s,%s,%s,%s, " \
                                 "%s,%s,%s,%s);"

    update_prep_cursor = db_connection.cursor(prepared=True)
    user_info_update_statement = "update user_info " \
                                 "set display_name = %s, email = %s, " \
                                 "orcid = %s, kb_internal_user = %s, " \
                                 "institution = %s, country = %s, " \
                                 "signup_date = %s, last_signin_date = %s " \
                                 "where username = %s;"

    new_user_info_count = 0
    users_info_updated_count = 0

    for username in user_stats_dict:
        #check if new user_info exists in the existing user info, if not insert the record.
        if username not in existing_user_info:
            input = (username,user_stats_dict[username]["name"],
                     user_stats_dict[username]["email"],user_stats_dict[username]["orcid"],
                     user_stats_dict[username]["kbase_internal_user"],
                     user_stats_dict[username]["institution"],user_stats_dict[username]["country"],
                     user_stats_dict[username]["signup_date"],user_stats_dict[username]["last_signin_date"])
            prep_cursor.execute(user_info_insert_statement,input)
            new_user_info_count+= 1
        else:
            #Check if anything has changed in the user_info, if so update the record
            if not ((user_stats_dict[username]["last_signin_date"] is None or 
                     user_stats_dict[username]["last_signin_date"].strftime("%Y-%m-%d %H:%M:%S") == 
                     str(existing_user_info[username]["last_signin_date"])) and
                    (user_stats_dict[username]["signup_date"].strftime("%Y-%m-%d %H:%M:%S") ==
                     str(existing_user_info[username]["signup_date"])) and
                    user_stats_dict[username]["country"] == existing_user_info[username]["country"] and
                    user_stats_dict[username]["institution"] == 
                    existing_user_info[username]["institution"] and
                    user_stats_dict[username]["kbase_internal_user"] == 
                    existing_user_info[username]["kb_internal_user"] and
                    user_stats_dict[username]["orcid"] == existing_user_info[username]["orcid"] and
                    user_stats_dict[username]["email"] == existing_user_info[username]["email"] and
                    user_stats_dict[username]["name"] == existing_user_info[username]["name"]):
                input = (user_stats_dict[username]["name"],user_stats_dict[username]["email"],
                         user_stats_dict[username]["orcid"],
                         user_stats_dict[username]["kbase_internal_user"],
                         user_stats_dict[username]["institution"],user_stats_dict[username]["country"],
                         user_stats_dict[username]["signup_date"],
                         user_stats_dict[username]["last_signin_date"],username)
                update_prep_cursor.execute(user_info_update_statement,input)
                users_info_updated_count += 1                    
    db_connection.commit()

    print("Number of new users info inserted:" + str(new_user_info_count))    
    print("Number of users updated:" + str(users_info_updated_count))    

    #NOW DO USER SUMMARY STATS
    user_summary_stats_insert_statement = "insert into user_system_summary_stats " \
                                 "(username,num_orgs, narrative_count, " \
                                 "shared_count, narratives_shared) " \
                                 "values(%s,%s,%s,%s,%s);"

    existing_user_summary_stats = dict()
    query = "select username, num_orgs, narrative_count, shared_count, narratives_shared " \
            "from user_system_summary_stats_current"
    cursor.execute(query)
    for (username, num_orgs, narrative_count, shared_count, narratives_shared) in cursor:
        existing_user_summary_stats[username]={"num_orgs":num_orgs,
                                       "narrative_count":narrative_count,
                                       "shared_count":shared_count,
                                       "narratives_shared":narratives_shared}
    print("Number of existing user summaries:" + str(len(existing_user_summary_stats)))

    new_user_summary_count= 0
    existing_user_summary_count= 0
    for username in user_stats_dict:
        if username not in existing_user_summary_stats:
            #if user does not exist insert
            input = (username,user_stats_dict[username]["num_orgs"],
                     user_stats_dict[username]["narrative_count"],user_stats_dict[username]["shared_count"],
                     user_stats_dict[username]["narratives_shared"])
            prep_cursor.execute(user_summary_stats_insert_statement,input)
            new_user_summary_count+= 1
        else:
            #else see if the new data differs from the most recent snapshot. If it does differ, do an insert
            if not (user_stats_dict[username]["num_orgs"] == 
                    existing_user_summary_stats[username]["num_orgs"] and 
                    user_stats_dict[username]["narrative_count"] == 
                    existing_user_summary_stats[username]["narrative_count"] and 
                    user_stats_dict[username]["shared_count"] == 
                    existing_user_summary_stats[username]["shared_count"] and 
                    user_stats_dict[username]["narratives_shared"] == 
                    existing_user_summary_stats[username]["narratives_shared"]):
                input = (username,user_stats_dict[username]["num_orgs"],
                         user_stats_dict[username]["narrative_count"],user_stats_dict[username]["shared_count"],
                         user_stats_dict[username]["narratives_shared"])
                prep_cursor.execute(user_summary_stats_insert_statement,input)
                existing_user_summary_count+= 1

    db_connection.commit()

    # THIS CODE is to update any of the 434 excluded users that had accounts made for them
    # but never logged in. In case any of them ever do log in, they will be removed from
    # the excluded list
    query = "UPDATE metrics.user_info set exclude = False where last_signin_date is not NULL"
    cursor.execute(query)
    db_connection.commit()

    print("Number of new users summary inserted:" + str(new_user_summary_count))    
    print("Number of existing users summary inserted:" + str(existing_user_summary_count))    

    return 1



upload_workspace_stats()

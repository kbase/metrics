from pymongo import MongoClient
from pymongo import ReadPreference
import json as _json
import os
import mysql.connector as mysql
from pprint import pprint

# NOTE get_user_info_from_auth2 sets up the initial dict. 
#The following functions update certain fields in the dict.
# So get_user_info_from_auth2 must be called before get_internal_users and get_user_orgs_count

metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']
mongoDB_metrics_connection = os.environ['MONGO_PATH']

sql_host = os.environ['SQL_HOST']
query_on = os.environ['QUERY_ON']

to_workspace =  os.environ['WRK_SUFFIX']

def get_non_kbase_staff():
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
    non_kb_staff = set()
    query = "select username from metrics.user_info where kb_internal_user = 0;"
    cursor.execute(query)
    for (username) in cursor:
        non_kb_staff.add(username[0])
    print(str(non_kb_staff))
    return non_kb_staff

def get_narratives_first_save():
    """ get the first save all narratives and put into monthly buckets """

    client_workspace = MongoClient(mongoDB_metrics_connection+to_workspace)
    db_workspace = client_workspace.workspace
    
    non_kb_staff = get_non_kbase_staff()
    workspace_nar_count = dict()
    legitimate_narratives_set = set()
    double_check_narratives_set = set()
    legitimate_narratives_found_set = set()
    monthly_counts = dict() #key is year-mon, value is number of narratives first saved in that month
    
    #Get all the legitimate narratives and and their respective user (not del, saved(not_temp))
    all_nar_cursor = db_workspace.workspaces.find({"del" : False,
                                                   "meta" : {"k" : "is_temporary", "v" : "false"} },
                                                  {"ws":1,"owner":1,"_id":0})
    for record in all_nar_cursor:
        if record["owner"] in non_kb_staff:
            legitimate_narratives_set.add(record["ws"])

    double_check_narrative_cur = db_workspace.workspaces.find({"del" : False, 
                                                               "meta": {"$elemMatch":{"$or":[{"k": "narrative"},{"k": "narrative_nice_name"}]}}},
                                                              {"ws":1,"owner":1,"_id":0})
    for record in double_check_narrative_cur:
        if record["owner"] in non_kb_staff:
            double_check_narratives_set.add(record["ws"])

    leg_minus_double_nars = legitimate_narratives_set - double_check_narratives_set
    double_minus_leg_nars = double_check_narratives_set - legitimate_narratives_set

    #NOTE THE TYPES OF NARRATIVES WILL NEED TO BE UPDATED AS MORE GET ADDED.
    narrative_ws_cursor = db_workspace.workspaceObjVersions.find({"type":
                                                                  {"$in":
                                                                   [
                                                                    "KBaseNarrative.Narrative-1.0",
                                                                    "KBaseNarrative.Narrative-2.0",
                                                                    "KBaseNarrative.Narrative-3.0",
                                                                    "KBaseNarrative.Narrative-4.0"]}, 
                                                                  "ver":1},{"ws":1,"savedate":1,"_id":0})
    for record in narrative_ws_cursor:
        if record['ws'] in legitimate_narratives_set:
            if record['ws'] in workspace_nar_count:
                workspace_nar_count[record['ws']] += 1
            else:
                workspace_nar_count[record['ws']] = 1
            legitimate_narratives_found_set.add(record['ws'])
#            legitimate_narratives_set.remove(record['ws'])
            date_elements = str(record["savedate"]).split("-")
            month_key = date_elements[0] + "-" + date_elements[1]
            if month_key in monthly_counts:
                monthly_counts[month_key] += 1
            else:
                monthly_counts[month_key] = 0
    print("MONTHLY COUNTS")
    pprint(monthly_counts)

    print("Leg_minus_double : " +str(leg_minus_double_nars))
    print("double_minus_leg : " +str(double_minus_leg_nars))
    print("Leg_minus_double size: " +str(len(leg_minus_double_nars)))
    print("double_minus_leg size: " +str(len(double_minus_leg_nars)))


    print("Accounted for narratives lenth : " + str(len(legitimate_narratives_found_set)))
    print("Unaccounted for narratives lenth : " + str(len(legitimate_narratives_set)))

    for i in sorted (monthly_counts.keys()):
        print(i + "\t" + str(monthly_counts[i]))

    print("Total number of Narratives:" + str(len(workspace_nar_count)));
    print("WORKSPACES WITH MORE THAN ONE FIRST NARRATIVE VERSION");
    ws_with_multiple_narratives = 0
    total_narratives_from_ws_with_multiple_narratives = 0
    for ws in workspace_nar_count:
        if workspace_nar_count[ws] > 1:
            print("WS : " + str(ws) + " has " + str(workspace_nar_count[ws]) + " different narratives in it")
            ws_with_multiple_narratives += 1
            total_narratives_from_ws_with_multiple_narratives += workspace_nar_count[ws]

    print("Total number of workspaces with multiple version 1 narratives : " + str(ws_with_multiple_narratives))
    print("Total number of narratives from workspaces with multiple version 1 narratives : " + str(total_narratives_from_ws_with_multiple_narratives))

get_narratives_first_save()

def upload_user_data(user_stats_dict):
    """
    Takes the User Stats dict that is populated by the other functions and 
    then populates the user_info and user_system_summary_stats tables
    in the metrics MySQL DB.
    """
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




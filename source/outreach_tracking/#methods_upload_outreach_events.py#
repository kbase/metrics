from pymongo import MongoClient
from pymongo import ReadPreference
import json as _json
import os
import mysql.connector as mysql
import requests

requests.packages.urllib3.disable_warnings()

# NOTE get_user_info_from_auth2 sets up the initial dict.
# The following functions update certain fields in the dict.
# So get_user_info_from_auth2 must be called before get_internal_users and get_user_orgs_count

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
mongoDB_metrics_connection = os.environ["MONGO_PATH"]

profile_url = os.environ["PROFILE_URL"]
kb_internal_user_url = os.environ["KB_INTERNAL_USER_URL"]
sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]

def get_internal_users(user_stats_dict):
    """
    Gets the internal users from the kb_internal_staff google sheet that Roy maintains.
    """
    params = (("tqx", "out:csv"), ("sheet", "KBaseStaffAssociatedUsernamesPastPresent"))
    response = requests.get(kb_internal_user_url, params=params)
    if response.status_code != 200:
        print(
            "ERROR - KB INTERNAL USER GOOGLE SHEET RESPONSE STATUS CODE : "
            + str(response.status_code)
        )
        print(
            "KB INTERNAL USER will not get updated until this is fixed. Rest of the uuser upload should work."
        )
        return user_stats_dict
    lines = response.text.split("\n")
    if len(lines) < 390:
        print(
            "SOMETHING IS WRONG WITH KBASE INTERNAL USERS LIST: "
            + str(response.status_code)
        )
    users_not_found_count = 0
    for line in lines:
        elements = line.split(",")
        user = elements[0][1:-1]
        if user in user_stats_dict:
            user_stats_dict[user]["kbase_internal_user"] = True
        else:
            users_not_found_count += 1
    if users_not_found_count > 0:
        print(
            "NUMBER OF USERS FOUND IN KB_INTERNAL GOOGLE SHEET THAT WERE NOT FOUND IN THE AUTH2 RECORDS : "
            + str(users_not_found_count)
        )
    return user_stats_dict


def upload_user_data(user_stats_dict):
    """
    Takes the User Stats dict that is populated by the other functions and 
    then populates the user_info and user_system_summary_stats tables
    in the metrics MySQL DB.
    """
    total_users = len(user_stats_dict.keys())
    rows_info_inserted = 0
    rows_info_updated = 0
    rows_stats_inserted = 0
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    # get all existing users
    existing_user_info = dict()
    query = (
        "select username, display_name, email, orcid, kb_internal_user, institution, "
        "country, signup_date, last_signin_date from user_info"
    )
    cursor.execute(query)
    for (
        username,
        display_name,
        email,
        orcid,
        kb_internal_user,
        institution,
        country,
        signup_date,
        last_signin_date,
    ) in cursor:
        existing_user_info[username] = {
            "name": display_name,
            "email": email,
            "orcid": orcid,
            "kb_internal_user": kb_internal_user,
            "institution": institution,
            "country": country,
            "signup_date": signup_date,
            "last_signin_date": last_signin_date,
        }

    print("Number of existing users:" + str(len(existing_user_info)))

    prep_cursor = db_connection.cursor(prepared=True)
    user_info_insert_statement = (
        "insert into user_info "
        "(username,display_name,email,orcid,kb_internal_user, "
        "institution,country,signup_date,last_signin_date) "
        "values(%s,%s,%s,%s,%s, "
        "%s,%s,%s,%s);"
    )

    update_prep_cursor = db_connection.cursor(prepared=True)
    user_info_update_statement = (
        "update user_info "
        "set display_name = %s, email = %s, "
        "orcid = %s, kb_internal_user = %s, "
        "institution = %s, country = %s, "
        "signup_date = %s, last_signin_date = %s "
        "where username = %s;"
    )

    new_user_info_count = 0
    users_info_updated_count = 0

    for username in user_stats_dict:
        # check if new user_info exists in the existing user info, if not insert the record.
        if username not in existing_user_info:
            input = (
                username,
                user_stats_dict[username]["name"],
                user_stats_dict[username]["email"],
                user_stats_dict[username]["orcid"],
                user_stats_dict[username]["kbase_internal_user"],
                user_stats_dict[username]["institution"],
                user_stats_dict[username]["country"],
                user_stats_dict[username]["signup_date"],
                user_stats_dict[username]["last_signin_date"],
            )
            prep_cursor.execute(user_info_insert_statement, input)
            new_user_info_count += 1
        else:
            # Check if anything has changed in the user_info, if so update the record
            if not (
                (
                    user_stats_dict[username]["last_signin_date"] is None
                    or user_stats_dict[username]["last_signin_date"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    == str(existing_user_info[username]["last_signin_date"])
                )
                and (
                    user_stats_dict[username]["signup_date"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    == str(existing_user_info[username]["signup_date"])
                )
                and user_stats_dict[username]["country"]
                == existing_user_info[username]["country"]
                and user_stats_dict[username]["institution"]
                == existing_user_info[username]["institution"]
                and user_stats_dict[username]["kbase_internal_user"]
                == existing_user_info[username]["kb_internal_user"]
                and user_stats_dict[username]["orcid"]
                == existing_user_info[username]["orcid"]
                and user_stats_dict[username]["email"]
                == existing_user_info[username]["email"]
                and user_stats_dict[username]["name"]
                == existing_user_info[username]["name"]
            ):
                input = (
                    user_stats_dict[username]["name"],
                    user_stats_dict[username]["email"],
                    user_stats_dict[username]["orcid"],
                    user_stats_dict[username]["kbase_internal_user"],
                    user_stats_dict[username]["institution"],
                    user_stats_dict[username]["country"],
                    user_stats_dict[username]["signup_date"],
                    user_stats_dict[username]["last_signin_date"],
                    username,
                )
                update_prep_cursor.execute(user_info_update_statement, input)
                users_info_updated_count += 1
    db_connection.commit()

    print("Number of new users info inserted:" + str(new_user_info_count))
    print("Number of users updated:" + str(users_info_updated_count))

    # NOW DO USER SUMMARY STATS
    user_summary_stats_insert_statement = (
        "insert into user_system_summary_stats "
        "(username,num_orgs, narrative_count, "
        "shared_count, narratives_shared) "
        "values(%s,%s,%s,%s,%s);"
    )

    existing_user_summary_stats = dict()
    query = (
        "select username, num_orgs, narrative_count, shared_count, narratives_shared "
        "from user_system_summary_stats_current"
    )
    cursor.execute(query)
    for (
        username,
        num_orgs,
        narrative_count,
        shared_count,
        narratives_shared,
    ) in cursor:
        existing_user_summary_stats[username] = {
            "num_orgs": num_orgs,
            "narrative_count": narrative_count,
            "shared_count": shared_count,
            "narratives_shared": narratives_shared,
        }
    print("Number of existing user summaries:" + str(len(existing_user_summary_stats)))

    new_user_summary_count = 0
    existing_user_summary_count = 0
    for username in user_stats_dict:
        if username not in existing_user_summary_stats:
            # if user does not exist insert
            input = (
                username,
                user_stats_dict[username]["num_orgs"],
                user_stats_dict[username]["narrative_count"],
                user_stats_dict[username]["shared_count"],
                user_stats_dict[username]["narratives_shared"],
            )
            prep_cursor.execute(user_summary_stats_insert_statement, input)
            new_user_summary_count += 1
        else:
            # else see if the new data differs from the most recent snapshot. If it does differ, do an insert
            if not (
                user_stats_dict[username]["num_orgs"]
                == existing_user_summary_stats[username]["num_orgs"]
                and user_stats_dict[username]["narrative_count"]
                == existing_user_summary_stats[username]["narrative_count"]
                and user_stats_dict[username]["shared_count"]
                == existing_user_summary_stats[username]["shared_count"]
                and user_stats_dict[username]["narratives_shared"]
                == existing_user_summary_stats[username]["narratives_shared"]
            ):
                input = (
                    username,
                    user_stats_dict[username]["num_orgs"],
                    user_stats_dict[username]["narrative_count"],
                    user_stats_dict[username]["shared_count"],
                    user_stats_dict[username]["narratives_shared"],
                )
                prep_cursor.execute(user_summary_stats_insert_statement, input)
                existing_user_summary_count += 1

    db_connection.commit()

    # THIS CODE is to update any of the 434 excluded users that had accounts made for them
    # but never logged in. In case any of them ever do log in, they will be removed from
    # the excluded list
    query = "UPDATE metrics.user_info set exclude = False where last_signin_date is not NULL"
    cursor.execute(query)
    db_connection.commit()

    print("Number of new users summary inserted:" + str(new_user_summary_count))
    print(
        "Number of existing users summary inserted:" + str(existing_user_summary_count)
    )

    return 1

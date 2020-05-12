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

to_auth2 = os.environ["AUTH2_SUFFIX"]
to_groups = os.environ["GRP_SUFFIX"]
to_workspace = os.environ["WRK_SUFFIX"]


def get_user_info_from_auth2():
    """ get auth2 info and kbase_internal_users. Creates initial dict for the data. """

    client_auth2 = MongoClient(mongoDB_metrics_connection + to_auth2)
    db_auth2 = client_auth2.auth2

    user_stats_dict = {}  # dict that will have userid as the key,
    # value is a dict with name, signup_date, last_signin_date,
    # and email (that gets values from this function)
    # orcid may be present and populated by this function.
    # later called functions will populate kbase_internal_user, num_orgs and ...

    user_info_query = db_auth2.users.find(
        {}, {"_id": 0, "user": 1, "email": 1, "display": 1, "create": 1, "login": 1}
    )
    for record in user_info_query:
        if record["user"] == "***ROOT***":
            continue
        user_stats_dict[record["user"]] = {
            "name": record["display"],
            "signup_date": record["create"],
            "last_signin_date": record["login"],
            "email": record["email"],
            "kbase_internal_user": False,
            "institution": None,
            "country": None,
            "orcid": None,
            "num_orgs": 0,
            "narrative_count": 0,
            "shared_count": 0,
            "narratives_shared": 0,
        }

    # Get all users with an ORCID authentication set up.
    users_orcid_query = db_auth2.users.find(
        {"idents.prov": "OrcID"},
        {"user": 1, "idents.prov": 1, "idents.prov_id": 1, "_id": 0},
    )
    for record in users_orcid_query:
        for ident in record["idents"]:
            if ident["prov"] == "OrcID":
                # just use the first orcid seen.
                user_stats_dict[record["user"]]["orcid"] = ident["prov_id"]
                continue

    client_auth2.close()
    return user_stats_dict


def get_internal_users(user_stats_dict):
    """
    Gets the internal users from the kb_internal_staff google sheet that Roy maintains.
    """
    params = (("tqx", "out:csv"), ("sheet", "KBaseStaffAccounts"))
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


def get_user_orgs_count(user_stats_dict):
    """ Gets the count of the orgs that users belong to and populates the onging data structure"""

    client_orgs = MongoClient(mongoDB_metrics_connection + to_groups)
    db_orgs = client_orgs.groups
    orgs_query = db_orgs.groups.find({}, {"name": 1, "memb.user": 1, "_id": 0})
    for record in orgs_query:
        for memb in record["memb"]:
            if memb["user"] in user_stats_dict:
                user_stats_dict[memb["user"]]["num_orgs"] += 1
    client_orgs.close()
    return user_stats_dict


def get_user_narrative_stats(user_stats_dict):
    """
    gets narrative summary stats (number of naratives, 
    number of shares, number of narratives shared for each user
    """
    client_workspace = MongoClient(mongoDB_metrics_connection + to_workspace)
    db_workspace = client_workspace.workspace
    ws_user_dict = {}
    # Get all the legitimate narratives and and their respective user (not del, saved(not_temp))
    all_nar_cursor = db_workspace.workspaces.find(
        {"del": False, "meta": {"k": "is_temporary", "v": "false"}},
        {"owner": 1, "ws": 1, "name": 1, "_id": 0},
    )
    for record in all_nar_cursor:
        # TO REMOVE OLD WORKSPACE METHOD OF 1 WS for all narratives.
        if "name" in record and record["name"] == record["owner"] + ":home":
            continue
        # narrative to user mapping
        ws_user_dict[record["ws"]] = record["owner"]
        # increment user narrative count
        user_stats_dict[record["owner"]]["narrative_count"] += 1

    # Get all the narratives that have been shared and how many times they have been shared.
    aggregation_string = [
        {"$match": {"perm": {"$in": [10, 20, 30]}}},
        {"$group": {"_id": "$id", "shared_count": {"$sum": 1}}},
    ]
    all_shared_perms_cursor = db_workspace.workspaceACLs.aggregate(aggregation_string)

    for record in db_workspace.workspaceACLs.aggregate(aggregation_string):
        if record["_id"] in ws_user_dict:
            user_stats_dict[ws_user_dict[record["_id"]]]["shared_count"] += record[
                "shared_count"
            ]
            user_stats_dict[ws_user_dict[record["_id"]]]["narratives_shared"] += 1

    return user_stats_dict


def get_institution_and_country(user_stats_dict):
    """
    Gets the institution and country information for the user from the profile information
    """
    url = profile_url
    headers = dict()
    arg_hash = {
        "method": "UserProfile.get_user_profile",
        "params": [list(user_stats_dict.keys())],
        "version": "1.1",
        "id": 123,
    }
    body = _json.dumps(arg_hash)
    timeout = 1800
    trust_all_ssl_certificates = 1

    ret = requests.post(
        url,
        data=body,
        headers=headers,
        timeout=timeout,
        verify=not trust_all_ssl_certificates,
    )
    ret.encoding = "utf-8"
    if ret.status_code == 500:
        if ret.headers.get(_CT) == _AJ:
            err = ret.json()
            if "error" in err:
                raise Exception(err)
            else:
                raise ServerError("Unknown", 0, ret.text)
        else:
            raise ServerError("Unknown", 0, ret.text)
    if not ret.ok:
        ret.raise_for_status()
    resp = ret.json()
    if "result" not in resp:
        raise ServerError("Unknown", 0, "An unknown server error occurred")
    print(str(len(resp["result"][0])))
    replaceDict = {"-": " ", ")": " ", ".": " ", "(": "", "/": "", ",": "", " +": " "}
    counter = 0
    for obj in resp["result"][0]:
        if obj is None:
            continue
        counter += 1

        if obj["user"]["username"] in user_stats_dict:
            user_stats_dict[obj["user"]["username"]]["country"] = obj["profile"][
                "userdata"
            ].get("country")
            institution = obj["profile"]["userdata"].get("organization")
            if institution == None:
                if "affiliations" in obj["profile"]["userdata"]:
                    affiliations = obj["profile"]["userdata"]["affiliations"]
                    try:
                        institution = affiliations[0]["organization"]
                    except IndexError:
                        try:
                            institution = obj["profile"]["userdata"]["organization"]
                        except:
                            pass
            if institution:
                for key, replacement in replaceDict.items():
                    # institution = institution.str.replace(key, replacement)
                    institution = institution.replace(key, replacement)
                institution = institution.rstrip()
            user_stats_dict[obj["user"]["username"]]["institution"] = institution
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

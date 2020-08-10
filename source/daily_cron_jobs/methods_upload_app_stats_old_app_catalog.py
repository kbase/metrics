# GetAppStats
#
import requests
import os
import datetime, time
import mysql.connector as mysql
from biokbase.catalog.Client import Catalog
from biokbase.narrative_method_store.client import NarrativeMethodStore

requests.packages.urllib3.disable_warnings()


catalog = Catalog(url=os.environ["CATALOG_URL"], token=os.environ["METRICS_USER_TOKEN"])
nms = NarrativeMethodStore(url=os.environ["NARRATIVE_METHOD_STORE"])
sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]

# Insures all finish times within last day.
yesterday = datetime.date.today() - datetime.timedelta(days=1)


def get_user_app_stats(
    start_date=datetime.datetime.combine(yesterday, datetime.datetime.min.time()),
    end_date=datetime.datetime.combine(yesterday, datetime.datetime.max.time()),
):
    """ 
    Gets a data dump from the app cataloge for a certain date window.   
    If no statt and end date are entered it will default to the last 15 calendar days (UTC TIME).
    It is 15 days because it uses an underlying method that 
    filters by creation_time and not finish_time
    """
    # From str to datetime, defaults to zero time.
    if type(start_date) == str:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    # Due to issue with method filtering only by creation_time need to grab
    # all 14 days before begin date to insure getting all records with a possible
    # finish_time within the time window specified. (14 days, 24 hours, 60 mins, 60 secs)
    begin = int(start_date.strftime("%s")) - (14 * 24 * 60 * 60)
    end = int(end_date.strftime("%s"))
    # print("BEGIN: " + str(begin))
    # print("END: " + str(end))

    time_interval = {"begin": begin, "end": end}
    stats = catalog.get_exec_raw_stats(time_interval)
    return stats


def helper_concatenation(var_pre, var_post):
    """ Simple helper method for concatenationg fields (Module and app/func name) """
    return_val = None
    if var_pre is None:
        var_pre = "Not Specified"
    if var_post is None:
        var_post = "Not Specified"
    if var_pre != "Not Specified" or var_post != "Not Specified":
        return_val = var_pre + "/" + var_post
    return return_val


def upload_user_app_stats(start_date=None, end_date=None):
    """ 
    Uploads the catalog app records into the MySQL back end.
    Uses the other functions 
    """
    if start_date is not None or end_date is not None:
        if start_date is not None and end_date is not None:
            app_usage_list = get_user_app_stats(start_date, end_date)
        else:
            raise ValueError("If start_date or end_date is set, then both must be set.")
    else:
        app_usage_list = get_user_app_stats()

    metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    prep_cursor = db_connection.cursor(prepared=True)
    user_app_insert_statement = (
        "insert into user_app_usage_old_app_catalog "
        "(job_id, username, app_name, "
        "start_date, finish_date, "
        "run_time, queue_time, is_error, git_commit_hash, func_name) "
        "values(%s,%s,%s,FROM_UNIXTIME(%s),FROM_UNIXTIME(%s),%s,%s,%s,%s,%s);"
    )

    check_if_first_run = "select count(*) from user_app_usage_old_app_catalog"
    cursor.execute(check_if_first_run)
    num_previous_records = 0
    for row in cursor:
        num_previous_records = row[0]

    check_no_job_id_duplicate_record_cursor = db_connection.cursor(prepared=True)
    check_dup_no_job_id_statement = (
        "select count(*) from user_app_usage_old_app_catalog "
        "where job_id is NULL "
        "and username = %s "
        "and app_name = %s "
        "and start_date = FROM_UNIXTIME(%s) "
        "and finish_date = FROM_UNIXTIME(%s) "
        "and run_time = %s "
        "and queue_time = %s "
        "and is_error = %s "
        "and git_commit_hash = %s "
        "and func_name = %s "
    )

    check_dup_no_job_id_no_app_name_statement = (
        "select count(*) from user_app_usage_old_app_catalog "
        "where job_id is NULL "
        "and username = %s "
        "and app_name is NULL "
        "and start_date = FROM_UNIXTIME(%s) "
        "and finish_date = FROM_UNIXTIME(%s) "
        "and run_time = %s "
        "and queue_time = %s "
        "and is_error = %s "
        "and git_commit_hash = %s "
        "and func_name = %s "
    )

    num_rows_inserted = 0
    num_rows_failed_duplicates = 0
    num_no_job_id = 0
    num_no_job_id_duplicate = 0
    # insert each record.
    for record in app_usage_list:
        is_error = False
        if record["is_error"] == 1:
            is_error = True
        input = [
            record.get("job_id"),
            record["user_id"],
            helper_concatenation(record["app_module_name"], record["app_id"]),
            round(record["exec_start_time"]),
            round(record["finish_time"]),
            round((record["finish_time"] - record["exec_start_time"])),
            round((record["exec_start_time"] - record["creation_time"])),
            is_error,
            record["git_commit_hash"],
            helper_concatenation(record["func_module_name"], record["func_name"]),
        ]
        # if not doing clean wiped insert, check for duplicates with job_id is null (some with app_name is Null)
        if "job_id" not in record:
            num_no_job_id += 1
            if num_previous_records > 0:
                check_input = input[1:]
                if (
                    helper_concatenation(record["app_module_name"], record["app_id"])
                    is None
                ):
                    # Don't need app_name
                    del check_input[1:2]
                    check_no_job_id_duplicate_record_cursor.execute(
                        check_dup_no_job_id_no_app_name_statement, check_input
                    )
                else:
                    check_no_job_id_duplicate_record_cursor.execute(
                        check_dup_no_job_id_statement, check_input
                    )

                dup_count = 0
                for row in check_no_job_id_duplicate_record_cursor:
                    dup_count = row[0]
                if int(dup_count) > 0:
                    num_no_job_id_duplicate += 1
                    # IT IS A DUPLICATE NO JOB ID RECORD. DO NOT DO AN INSERT
                    continue

        # Error handling from https://www.programcreek.com/python/example/93043/mysql.connector.Error
        try:
            prep_cursor.execute(user_app_insert_statement, input)
            num_rows_inserted += 1
        except mysql.Error as err:
            # print("ERROR: " + str(err))
            # print("Duplicate Input: " + str(input))
            num_rows_failed_duplicates += 1

    db_connection.commit()
    print("Number of app records inserted : " + str(num_rows_inserted))
    print("Number of app records duplicate : " + str(num_rows_failed_duplicates))
    print("Number of no job id records : " + str(num_no_job_id))
    print("Number of no job id records skipped: " + str(num_no_job_id_duplicate))
    print("App Usage Record_count: " + str(len(app_usage_list)))
    return 1

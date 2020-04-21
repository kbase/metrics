# GetAppStats
#
import requests
import os
import datetime, time
import mysql.connector as mysql
from biokbase.catalog.Client import Catalog
from biokbase.narrative_method_store.client import NarrativeMethodStore

requests.packages.urllib3.disable_warnings()
"""
THIS IS A SCRIPT MADE TO BACKFILL THE QUEUE TIMES FOR THE APP STATS RETRIEVED FROM APP CATALOG.
THIS PROBABLY ONLY NEEDED TO BE RUN THE ONE TIME, AND WILL NOT BE RELEVANT ONCE WE SWITCH OVER TO EE2.
"""

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

    #    time_interval = {'begin': begin , 'end': end}
    time_interval = {"begin": 0, "end": end}
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


def backfill_queue_times_in_app_stats(start_date=None, end_date=None):
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

    #    print("RECORD: " + str(app_usage_list[0]))
    #    return 1;

    metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    wo_job_prep_cursor = db_connection.cursor(prepared=True)
    update_queue_times_without_job_id = (
        "update metrics.user_app_usage "
        "set queue_time = %s "
        "where job_id is null and username = %s "
        "and app_name = %s "
        "and start_date = FROM_UNIXTIME(%s) "
        "and finish_date - FROM_UNIXTIME(%s) "
        "and run_time = %s and is_error = %s "
        "and git_commit_hash = %s and func_name = %s ;"
    )

    with_job_prep_cursor = db_connection.cursor(prepared=True)
    update_queue_times_with_job_id = (
        "update metrics.user_app_usage set queue_time = %s where job_id = %s ;"
    )

    num_wo_job_id_updates = 0
    num_with_job_id_updates = 0

    # update each record.
    for record in app_usage_list:
        is_error = False
        if record["is_error"] == 1:
            is_error = True
        if "job_id" not in record:
            input = [
                round((record["exec_start_time"] - record["creation_time"])),
                record["user_id"],
                helper_concatenation(record["app_module_name"], record["app_id"]),
                round(record["exec_start_time"]),
                round(record["finish_time"]),
                round((record["finish_time"] - record["exec_start_time"])),
                is_error,
                record["git_commit_hash"],
                helper_concatenation(record["func_module_name"], record["func_name"]),
            ]
            # DO update
            wo_job_prep_cursor.execute(update_queue_times_without_job_id, input)
            num_wo_job_id_updates += 1
        else:
            input = [
                round((record["exec_start_time"] - record["creation_time"])),
                record.get("job_id"),
            ]
            with_job_prep_cursor.execute(update_queue_times_with_job_id, input)
            num_with_job_id_updates += 1
    db_connection.commit()
    print("TOTAL WO JOB UPDATES : " + str(num_wo_job_id_updates))
    print("TOTAL WITH JOB UPDATES : " + str(num_with_job_id_updates))
    return 1


backfill_queue_times_in_app_stats()

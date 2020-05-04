# GetAppStats
#
import requests
import os
import datetime, time
import mysql.connector as mysql

from biokbase.narrative_method_store.client import NarrativeMethodStore
import biokbase.narrative.clients as clients
import datetime
from installed_clients.execution_engine2Client import execution_engine2

requests.packages.urllib3.disable_warnings()

# GetEE2AppStats
ee2 = execution_engine2(
    # CHANGE URL to production: https://kbase.us/services/ee2
    # CHANGE URL for CI: https://ci.kbase.us/services/ee2 (Need to change token in .env as well)
    # CHANGE URL for APPDEV: https://appdev.kbase.us/services/ee2
    url="https://kbase.us/services/ee2",
    token=os.environ["METRICS_USER_TOKEN"]
)

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
    Gets a data dump from EE2 for a certain date window.
    If no start and end date are entered it will default to the last 15 calendar days (UTC TIME).
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
    begin = (int(start_date.strftime("%s")) - (14 * 24 * 60 * 60)) * 1000
    end = int(end_date.strftime("%s")) * 1000
    statuses = ["queued", "terminated", "running", "created", "estimated"]
    job_array = []
    #    print("START DATE: " + str(start_date))
    #    print("END DATE: " + str(end_date))
    #    print("BEGIN: " + str(begin))
    #    print("END: " + str(end))
    # For params get finished jobs from execution engine
    params = {"start_time": begin, "end_time": end, "ascending": 0, "limit": 1000000}
    stats = ee2.check_jobs_date_range_for_all(params=params)
    print("LENGTH OF EE2 PULL : " + str(len(stats["jobs"])))
    has_queued_counter = 0
    no_queued_counter = 0
    has_requirements_counter = 0
    for job in stats["jobs"]:
        if job["status"] in statuses or "finished" not in job:
            continue
        else:
            if "running" not in job:
                print("Job Id did not have running: " + str(job["job_id"]))
                job["running"] = job["finished"]
            run_time = (job["finished"] - job["running"]) / 1000
            finished = datetime.datetime.fromtimestamp(job["finished"] / 1000)
            run_start = datetime.datetime.fromtimestamp(job["running"] / 1000)
            if "queued" in job:
                queue_time = (job["running"] - job["queued"]) / 1000
                has_queued_counter += 1
            else:
                queue_time = (job["running"] - job["created"]) / 1000
                no_queued_counter += 1

            ws_id = None
            if "wsid" in job:
                ws_id = job["wsid"]

            is_error = False
            if job["status"] == "error":
                is_error = True
            reserved_cpu = None
            if (
                "job_input" in job
                and "requirements" in job["job_input"]
                and "cpu" in job["job_input"]["requirements"]
            ):
                has_requirements_counter += 1
                reserved_cpu = job["job_input"]["requirements"]["cpu"]
            # For values present construct job stats dictionary and append to job array
            if "job_input" not in job:
                print("JOB ID : " + str(job["job_id"]) + " has no job_input ")
                print(str(job))
                exit()
            job_stats = {
                "job_id": job["job_id"],
                "user": job["user"],
                "finish_date": finished.strftime("%Y-%m-%d %H:%M:%S"),
                "start_date": run_start.strftime("%Y-%m-%d %H:%M:%S"),
                "run_time": run_time,
                "app_name": job["job_input"]["app_id"].replace(".", "/"),
                "func_name": job["job_input"]["method"].replace(".", "/"),
                "git_commit_hash": job["job_input"]["service_ver"],
                "is_error": is_error,
                "ws_id": ws_id,
                "queue_time": queue_time,
                "reserved_cpu": reserved_cpu,
            }
            job_array.append(job_stats)
    print("HAS QUEUED Count: " + str(has_queued_counter))
    print("NO QUEUED Count: " + str(no_queued_counter))
    print("HAS REQUIREMENTS Count: " + str(has_requirements_counter))
    return job_array

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

    print("Number of records in app list : " + str(len(app_usage_list)))
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
        "insert into metrics.user_app_usage_ee2_cpu "
        "(job_id, username, app_name, "
        "start_date, finish_date, "
        "run_time, queue_time, is_error, "
        "git_commit_hash, func_name, "
        "ws_id, reserved_cpu) "
        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    )

    check_if_first_run = "select count(*) from metrics.user_app_usage_ee2_cpu"
    cursor.execute(check_if_first_run)
    num_previous_records = 0
    for row in cursor:
        num_previous_records = row[0]

    check_no_job_id_duplicate_record_cursor = db_connection.cursor(prepared=True)
    check_dup_no_job_id_statement = (
        "select count(*) from metrics.user_app_usage_ee2_cpu "
        "where job_id is NULL "
        "and username = %s "
        "and app_name = %s "
        "and start_date = %s "
        "and finish_date = %s "
        "and run_time = %s "
        "and queue_time = %s "
        "and is_error = %s "
        "and git_commit_hash = %s "
        "and func_name = %s "
        "and ws_id = %s"
    )

    check_dup_no_job_id_no_app_name_statement = (
        "select count(*) from metrics.user_app_usage_ee2_cpu "
        "where job_id is NULL "
        "and username = %s "
        "and app_name is NULL "
        "and start_date = %s "
        "and finish_date = %s "
        "and run_time = %s "
        "and queue_time = %s "
        "and is_error = %s "
        "and git_commit_hash = %s "
        "and func_name = %s "
        "and ws_id = %s"
    )

    num_rows_inserted = 0
    num_rows_failed_duplicates = 0
    num_no_job_id = 0
    num_no_job_id_duplicate = 0
    # insert each record.
    for record in app_usage_list:
        input = [
            record.get("job_id"),
            record["user"],
            record["app_name"],
            record["start_date"],
            record["finish_date"],
            round(record["run_time"]),
            round((record["queue_time"])),
            record["is_error"],
            record["git_commit_hash"],
            record["func_name"],
            record["ws_id"],
            record["reserved_cpu"],
        ]
        # if not doing clean wiped insert, check for duplicates with job_id is null (some with app_name is Null)
        if "job_id" not in record:
            num_no_job_id += 1
            if num_previous_records > 0:
                check_input = input[1:-1]
                if record["app_name"] is None:
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

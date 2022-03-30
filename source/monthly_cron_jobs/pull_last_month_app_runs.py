# GetAppStats
#
import requests
import os

from datetime import date, timedelta, datetime
import mysql.connector as mysql
from biokbase.narrative_method_store.client import NarrativeMethodStore
#from source.daily_cron_jobs.installed_clients.execution_engine2Client import execution_engine2
from installed_clients.execution_engine2Client import execution_engine2

requests.packages.urllib3.disable_warnings()

ee2_url = os.environ["EE2_URL"]
# GetEE2AppStats
ee2 = execution_engine2(
    url=ee2_url,
    token=os.environ["METRICS_USER_TOKEN"],
)

nms = NarrativeMethodStore(url=os.environ["NARRATIVE_METHOD_STORE"])
sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]
metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]

def get_downloaders_set():
    db_connection = mysql.connect(
        host=sql_host,  # "mysql1", #"localhost",
        user="metrics",  # "root",
        passwd=metrics_mysql_password,
        database="metrics",  # "datacamp"
    )

    cursor = db_connection.cursor()


    #returns a set of downloadwer apps
    query = "select downloader_app_name, 1 from metrics.downloader_apps";
    cursor.execute(query)
    downloaders_set = set()
    for row_values in cursor:
        downloaders_set.add(row_values[0])
    print(str(downloaders_set))
    return downloaders_set


def pull_downloading_jobs(downloaders_set):
    #get first day of the month:
    #first_of_this_month = datetime.today().replace(day=1)
    date_today = datetime.now()
    first_of_this_month = date_today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    print(str(first_of_this_month))
    last_day_of_prev_month = first_of_this_month.replace(day=1) - timedelta(days=1)
    first_of_previous_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)

    #first_of_previous_month = datetime(first_of_previous_month).replace(hour=0, minute=0, second=0, microsecond=0)
    print(str(first_of_this_month))
    print(str(first_of_previous_month))
    end = int(first_of_this_month.strftime("%s")) * 1000
    begin = int(first_of_previous_month.strftime("%s")) * 1000
    print("End :" + str(end))
    print("Begin :" + str(begin))
    # begin = 0  # IF need to populate for all time.
    
    params = {"start_time": begin, "end_time": end, "ascending": 0, "limit": 1000000000}
    stats = ee2.check_jobs_date_range_for_all(params=params)

    statuses = ["queued", "terminated", "running", "created", "estimated","error"]
    finished_job_count = 0
    downloaders_count = 0
    downloaders_with_ws_id_count = 0
    in_if_count = 0
    downloader_results = dict()
    ws_ids_with_multiple = list()
    for job in stats["jobs"]:
        if job["status"] in statuses or "finished" not in job:
            continue
        else:
            # only want non errored finished jobs
            if "job_input" in job and "job_id" in job and "user" in job:
                in_if_count += 1
                method = job["job_input"]["method"]
                method = method.replace(".", "/")
                if method in downloaders_set:
                    downloaders_count += 1
                    ws__obj_id = None
                    if len(job["job_input"]['params']) > 0:
                        for param in job["job_input"]['params']:
                            if "input_ref" in param:
                                ws_obj_id = param["input_ref"]
                    if ws_id is not None: 
                        job_id = job["job_id"]
                        username = job["user"]
                        if ws_id not in downloader_results:
                            downloader_results[ws_obj_id] = dict()
                        else:
                            ws_ids_with_multiple.append(ws_id)
                        downloader_results[ws_obj_id][job_id] = username
                        downloaders_with_ws_id_count += 1
            finished_job_count += 1

#    print("Finished job count : "  + str(finished_job_count))
#    print("In If count : "  + str(in_if_count))
#    print("Downloaders job count : "  + str(downloaders_count))
    print("Downloaders with ws_id count : "  + str(downloaders_with_ws_id_count))

    print("DOWNLOADER RESULTS:")
    print(str(downloader_results))
    loop_count = 0
    for mult_dl_ws_id in ws_ids_with_multiple:
        loop_count +=1
        if loop_count > 5:
            break
        print("obj id: " + str(mult_dl_ws_id))
        print(str(downloader_results[mult_dl_ws_id]))
    
    return downloader_results

def main_function():
    downloaders_set = get_downloaders_set()
    downloader_results = pull_downloading_jobs(downloaders_set)
    return downloader_results

main_function()

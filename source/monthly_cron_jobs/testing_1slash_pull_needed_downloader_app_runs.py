# GetAppStats
#
import requests
import os

from datetime import date, timedelta, datetime
import mysql.connector as mysql
from biokbase.narrative_method_store.client import NarrativeMethodStore
#from source.daily_cron_jobs.installed_clients.execution_engine2Client import execution_engine2
from installed_clients.execution_engine2Client import execution_engine2

################################################
#
# This code is to pull the needed downloader app runs that may have been downloaders for DOI objects
# After the intitial pulling of all data (start time for pull will be zero)
# Subsequent runs will be doing the following logic to fetermine the start time for the window of
# grabbing app runs
# If there are new DOIs since the last time this has been run,
# then take minimum initial_save_date
# (from the metric_reporting.workspaces_current table, means this shoud be run after the workspaces monthly cron job)
# amongst the new DOI workspaces
# Take the earlier of the past month (default) and the new DOI workspaces initial save date.
#
################################################



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

db_connection = mysql.connect(
    host=sql_host,  # "mysql1", #"localhost",
    user="metrics",  # "root",
    passwd=metrics_mysql_password,
    database="metrics",  # "datacamp"
)

cursor = db_connection.cursor()

def get_minimum_date_for_new_doi_workspaces(cursor):
    #First Determine the default being the start of the previous month
    #get first day of the month:
    query = (
        "select min(initial_save_date) from metrics_reporting.workspaces_current where ws_id in ( "
        "   select ws_id from metrics.doi_ws_map "
        "   where ws_id not in (select unique ws_id from metrics.doi_metrics)) "
    )
    cursor.execute(query)
    min_new_doi_ws_date = None
    for row_values in cursor:
        min_new_doi_ws_date = row_values[0]
    print("MIN NEW DOI WS DATE:" + str(min_new_doi_ws_date))
    return min_new_doi_ws_date
    

    
def get_downloaders_set():
    #returns a set of downloadwer apps
    query = "select downloader_app_name, 1 from metrics.downloader_apps";
    cursor.execute(query)
    downloaders_set = set()
    for row_values in cursor:
        downloaders_set.add(row_values[0])
    print(str(downloaders_set))
    return downloaders_set


def pull_downloading_jobs(downloaders_set):
    # get first day of the month:
    # first_of_this_month = datetime.today().replace(day=1)
    date_today = datetime.now()
    first_of_this_month = date_today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    print(str(first_of_this_month))
    last_day_of_prev_month = first_of_this_month.replace(day=1) - timedelta(days=1)
    first_of_previous_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
    first_of_previous_month_begin = int(first_of_previous_month.strftime("%s")) * 1000

    #first_of_previous_month = datetime(first_of_previous_month).replace(hour=0, minute=0, second=0, microsecond=0)
    print(str(first_of_this_month))
    print(str(first_of_previous_month))
    end = int(first_of_this_month.strftime("%s")) * 1000
    prev_month_begin = int(first_of_previous_month.strftime("%s")) * 1000
    print("End :" + str(end))
    print("Prev Month Begin :" + str(prev_month_begin))
    begin = prev_month_begin
    
    # See if there new doi workspaces and get their minimum data
    min_new_doi_ws_date = get_minimum_date_for_new_doi_workspaces(cursor)
    print("Returned min_new_doi_ws_date : " + str(min_new_doi_ws_date))
    if min_new_doi_ws_date is not None:
        min_new_doi_ws_epoch = int(min_new_doi_ws_date.strftime("%s")) * 1000
        print("min_new_doi_ws_epoch : " + str(min_new_doi_ws_epoch))
        if (prev_month_begin > min_new_doi_ws_epoch):
            begin =  min_new_doi_ws_epoch
#    begin = 0  # IF need to populate for all time.
#   JANUARY 2022 ONKY
    begin = 1640995200000
    end = 1643673599000
    print("begin to be used : " + str(begin))
    
    params = {"start_time": begin, "end_time": end, "ascending": 0, "limit": 1000000000}
    stats = ee2.check_jobs_date_range_for_all(params=params)

    statuses = ["queued", "terminated", "running", "created", "estimated","error"]
    finished_job_count = 0
    downloaders_count = 0
    downloaders_with_ws_id_count = 0
    in_if_count = 0

    slash_1_methods = dict()
    all_methods = dict()
    slash_1_usernames = dict()
    slash_1_count = 0
    slash_2_count = 0
    
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
#                    if downloaders_count > 10:
#                        exit()
                    ws_obj_id = None
                    if len(job["job_input"]['params']) > 0:
                        for param in job["job_input"]['params']:
                            if "input_ref" in param:
                                ws_obj_id = param["input_ref"]
                    if ws_obj_id is not None: 
                        #job_id = job["job_id"]
                        username = job["user"]
                        if ws_obj_id not in downloader_results:
                            downloader_results[ws_obj_id] = set()
                            #downloader_results[ws_obj_id] = dict()
                        else:
                            ws_ids_with_multiple.append(ws_obj_id)
                        #downloader_results[ws_obj_id][job_id] = username
                        downloader_results[ws_obj_id].add(username)
                        downloaders_with_ws_id_count += 1

                        
                        slash_count = 0
#                        print("Testing WS_OBJ ID : " + str(ws_obj_id) + "    METHOD: " + str(method))
                        for char in ws_obj_id:
                            if char == "/":
                                slash_count+=1
#                        print("SLASH COUNT : " + str(slash_count))
                        if slash_count == 1:
                            slash_1_count += 1
                            if method not in slash_1_methods:
                                slash_1_methods[method] = 1
                            else:
                                slash_1_methods[method] += 1
                            if username not in slash_1_usernames:
                                slash_1_usernames[username] = 1
                            else:
                                slash_1_usernames[username] += 1
                        elif slash_count == 2:
                            slash_2_count += 1
                        if method not in all_methods:
                            all_methods[method] = 1
                        else:
                            all_methods[method] += 1
                                    






                        
            finished_job_count += 1


#    print("Finished job count : "  + str(finished_job_count))
#    print("In If count : "  + str(in_if_count))
#    print("Downloaders job count : "  + str(downloaders_count))

    print("Downloaders with ws_id count : "  + str(downloaders_with_ws_id_count))

    print("DOWNLOADER RESULTS:")
    print(str(downloader_results))
#    loop_count = 0
#    for mult_dl_ws_id in ws_ids_with_multiple:
#        loop_count +=1
#        if loop_count > 5:
#            break
#        print("obj id: " + str(mult_dl_ws_id))
#        print(str(downloader_results[mult_dl_ws_id]))


    count_2_slash_obj_id = 0
    count_1_slash_obj_id = 0
    bad_ws_obj_ids = set()
    for ws_obj_id in downloader_results:
        if len(downloader_results[ws_obj_id]) > 1:
            print("WS Obj ID: " + str(ws_obj_id) + " :: " + str(downloader_results[ws_obj_id])) 
        slash_count = 0
        for char in ws_obj_id:
            if char == "/":
                slash_count+=1
        if slash_count == 1:
            count_1_slash_obj_id += 1
        elif slash_count == 2:
            count_2_slash_obj_id += 1
        else:
            bad_ws_obj_ids.add(ws_obj_id)


    
            
    print("Bad ws obj ids : " + str(bad_ws_obj_ids))
    print("Total 1 slash count : " + str(count_1_slash_obj_id))
    print("Total 2 slash count : " + str(count_2_slash_obj_id))
    print("SLASH 1 Methods count: " + str(len(slash_1_methods)))
    running_total = 0
    for k in sorted(all_methods, key=all_methods.get, reverse=True):
        temp_method_slash_1_count = 0
        if k in slash_1_methods:
            temp_method_slash_1_count = slash_1_methods[k]
        print(str(k) + ":::" +str( temp_method_slash_1_count) + ":::" + str(all_methods[k]))
        running_total += temp_method_slash_1_count
    print("SLASH1_METHODS : " + str(slash_1_methods))
    print("ALL_METHODS : " + str(all_methods))
#    for k in sorted(slash_1_methods, key=slash_1_methods.get, reverse=True):
#        print(str(k))
#    for k in sorted(slash_1_methods, key=slash_1_methods.get, reverse=True):
#        print(str( slash_1_methods[k]))
    print("Running total: " + str(running_total))

#    print("SLASH 1 Usernames count: " + str(len(slash_1_usernames)))
#    for k in sorted(slash_1_usernames, key=slash_1_usernames.get, reverse=True):
#        print(str(k) + "\t" +str( slash_1_usernames[k]))
#    for k in sorted(slash_1_usernames, key=slash_1_usernames.get, reverse=True):
#        print(str(k))
#    for k in sorted(slash_1_usernames, key=slash_1_usernames.get, reverse=True):
#        print(str( slash_1_usernames[k]))
    
    return downloader_results

def main_function():
    downloaders_set = get_downloaders_set()
    downloader_results = pull_downloading_jobs(downloaders_set)
    return downloader_results

main_function()

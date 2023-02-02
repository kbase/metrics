# GetAppStats
#
import requests
import os
import time
from pymongo import MongoClient
from pymongo import ReadPreference

from datetime import date, timedelta, datetime
import mysql.connector as mysql
from biokbase.narrative_method_store.client import NarrativeMethodStore
#from source.daily_cron_jobs.installed_clients.execution_engine2Client import execution_engine2
from installed_clients.execution_engine2Client import execution_engine2

################################################
#
# This code is to pull the needed downloader app runs that may have been downloaders for DOI objects
#
################################################

requests.packages.urllib3.disable_warnings()

to_workspace = os.environ["WRK_SUFFIX"]

ee2_url = os.environ["EE2_URL"]
# GetEE2AppStats
ee2 = execution_engine2(
    url=ee2_url,
    token=os.environ["METRICS_USER_TOKEN"],
)

nms = NarrativeMethodStore(url=os.environ["NARRATIVE_METHOD_STORE"])
sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]

mongoDB_metrics_connection = os.environ["MONGO_PATH"]
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
        "   select ws_id from metrics.copy_doi_ws_map "
        "   where ws_id not in (select unique ws_id from metrics.copy_doi_metrics)) "
    )
    cursor.execute(query)
    min_new_doi_ws_date = None
    for row_values in cursor:
        min_new_doi_ws_date = row_values[0]
    print("MIN NEW DOI WS DATE:" + str(min_new_doi_ws_date))
    return min_new_doi_ws_date

def get_existing_problem_refs(cursor):
    # builds data structure for problematic references previously resolved
    query = (
        "select job_id, original_ref_id, resolved_ref_id "
        "from downloaders_problematic_obj_ids"
    )
    cursor.execute(query)
    problem_refs_lookup = dict()
    for row_values in cursor:
        job_id = row_values[0]
        original_ref_id = row_values[1]
        resolved_ref_id = row_values[2]
        if job_id not in problem_refs_lookup:
            problem_refs_lookup[job_id] = dict()
        problem_refs_lookup[job_id][original_ref_id] = resolved_ref_id
    return problem_refs_lookup
    
def get_minimum_date_for_doi_workspaces(cursor):
    # gets earliest initial save out of all the doi workspaces 
    query = (
        "select min(initial_save_date) from metrics_reporting.workspaces_current where ws_id in ( "
        "   select ws_id from metrics.copy_doi_ws_map) ")
    cursor.execute(query)
    min_new_doi_ws_date = None
    for row_values in cursor:
        min_doi_ws_date = row_values[0]
    print("MIN DOI WS DATE:" + str(min_new_doi_ws_date))
    return min_doi_ws_date


def get_downloaders_set(cursor):
    #returns a set of downloadwer apps
#    query = "select downloader_app_name, 1 from metrics.downloader_apps";
    query = (
        "select downloader_app_name as app_name from downloader_apps da "
        "union select distinct uau.func_name from user_app_usage uau "
        "where (uau.func_name like '%export%' or uau.func_name like '%download%' or "
        "uau.app_name like '%export%' or uau.app_name like '%download%' or "
        "uau.func_name like 'kb_ObjectInfo%' or uau.app_name like 'kb_ObjectInfo%') ")
#    query = (
#        "select downloader_app_name as app_name from downloader_apps da "
#        "union select uau.func_name from user_app_usage uau "
#        "where (uau.func_name like '%export%' or uau.func_name like '%download%' "
#        "or uau.app_name like '%export%' or uau.app_name like '%download%') ")

    cursor.execute(query)
    downloaders_set = set()
    for row_values in cursor:
        downloaders_set.add(row_values[0])
    print(str(downloaders_set))
    print("Number of downloaders : " + str(len(downloaders_set)))
    return downloaders_set

def pull_downloading_jobs(downloaders_set, problem_refs_lookup):

    client = MongoClient(mongoDB_metrics_connection + to_workspace)
    db = client.workspace

    prep_cursor = db_connection.cursor(prepared=True)
    downloaders_problematic_obj_ids_insert_statement = (
        "insert into downloaders_problematic_obj_ids "
        "(original_ref_id, resolved_ref_id, job_id) "
        "values(%s,%s, %s);")
    insert_prob_refs_count = 0
    
    statuses = ["queued", "terminated", "running", "created", "estimated","error"]
    finished_job_count = 0
    downloaders_count = 0
    downloading_jobs_with_orphaned_refs_count = 0
    downloading_triples_not_digits_count = 0
    downloaders_with_ws_id_count = 0
    in_if_count = 0

    downloaders_dict = dict()
    for downloader in downloaders_set:
        downloaders_dict[downloader] = dict()
        downloaders_dict[downloader]["has_input_ref_count"] = 0
        downloaders_dict[downloader]["no_input_ref_count"] = 0
    
    downloader_results = dict()
    # the data structure looks like downloaded_ws_obj_id => { downloader_username => [job_id]}
    has_2_elements_count = 0

    
    earliest_year = 2016
    today = date.today()
    current_year = int(today.year)
    part_of_year_list = (1,2,3,4)

    years_to_do = range(earliest_year,(current_year + 1))

    print("Current year : " + str(current_year))
    print("Years to do: " + str(years_to_do))

    fba_tools_bulk_export_objects_jobs = list()
    DataFileUtil_download_web_file_jobs = list()
    
    for year_to_do in years_to_do:
        # NEED TO CHUNK UP THE RESULTS BY QUARTER, OTHERWISE EE@ TIMESOUT.
        for part_of_year in part_of_year_list:
            if part_of_year == 1:
                begin = int(datetime(year_to_do, 1, 1, 0, 0).timestamp()) * 1000
                end = int(datetime(year_to_do, 3, 31, 23, 59).timestamp()) * 1000
            elif part_of_year == 2:
                begin = int(datetime(year_to_do, 4, 1, 0, 0).timestamp()) * 1000
                end = int(datetime(year_to_do, 6, 30, 23, 59).timestamp()) * 1000
            elif part_of_year == 3:
                begin = int(datetime(year_to_do, 7, 1, 0, 0).timestamp()) * 1000
                end = int(datetime(year_to_do, 9, 30, 23, 59).timestamp()) * 1000
            else:
                begin = int(datetime(year_to_do, 10, 1, 0, 0).timestamp()) * 1000
                end = int(datetime(year_to_do, 12, 31, 23, 59).timestamp()) * 1000 

            yearly_start_time = time.time()
            print("Yearly Quarter to do start: " + str(year_to_do) + "_" + str(part_of_year) + " :: " + str(yearly_start_time))
        
            params = {"start_time": begin, "end_time": end, "ascending": 0, "limit": 1000000000}
            stats = ee2.check_jobs_date_range_for_all(params=params)

            yearly_finished_count = 0
            yearly_downloader_count = 0

            example_counter = 0
            download_job_without_input_ref_count = 0

            kbObjectInfo_dict = dict()

            fba_tools_bulk_export_objects_job_count = 0
            
            for job in stats["jobs"]:
                if job["status"] in statuses or "finished" not in job:
                    continue
                else:
                    # only want non errored finished jobs
                    if "job_input" in job and "job_id" in job and "user" in job:
                        in_if_count += 1
                        method = job["job_input"]["method"]
                        app_id = job["job_input"]["app_id"]
                        method = method.replace(".", "/")
                        if method in downloaders_set or app_id in downloaders_set:
                            if method == "DataFileUtil/download_web_file":
                                DataFileUtil_download_web_file_jobs.append(job)
                            if method == "fba_tools/bulk_export_objects":
                                fba_tools_bulk_export_objects_jobs.append(job)
                                fba_tools_bulk_export_objects_job_count += 1
                            downloaders_count += 1
                            yearly_downloader_count += 1
                            ws_obj_id = None
                            job_id = job["job_id"]
                            needs_to_be_added_to_the_db = 1
                            if "kb_ObjectInfo" in method:
                                # need to find input ref differently
                                found_kb_info_ref = 0
                                for param_key in job["job_input"]["params"][0]:
                                    if "input_ref" in param_key:
                                        ws_obj_id = job["job_input"]["params"][0][param_key]
                                        kbObjectInfo_dict[job_id] = ws_obj_id
                                        found_kb_info_ref = 1
                                        print("IN kbObjectInfo_dict checking ws_obj_id: " + ws_obj_id )
                                if found_kb_info_ref == 0:
                                    print("######################")
                                    print("UNABLE TO FIND kbinfo job_id : " + str(job_id))
                                    print("######################")
                            elif len(job["job_input"]['params']) > 0:
                                for param in job["job_input"]['params']:
                                    if "input_ref" in param:
                                        ws_obj_id = param["input_ref"]
                            if ws_obj_id is not None:
                                downloaders_dict[method]["has_input_ref_count"] += 1
                                #job_id = job["job_id"]
                                username = job["user"]
                                used_ws_obj_id = None
#                                print("ws_obj_id : " + ws_obj_id)
                                elements = ws_obj_id.split("/")
                                if len(elements) == 3:
                                    if elements[0].isdigit() and elements[1].isdigit() and elements[2].isdigit():
                                        used_ws_obj_id = ws_obj_id
                                        needs_to_be_added_to_the_db = 0
                                    else:
                                        # had no cases of this as of this point will treat as orphaned?                                 
                                        # need to check at end to see if this code needs to be added.
                                        print("Unexpected triplet ref format not with digits: " + ws_obj_id)
                                        downloading_jobs_with_orphaned_refs_count += 1
                                        downloading_triples_not_digits_count += 1
                                elif job_id in problem_refs_lookup and ws_obj_id in problem_refs_lookup[job_id]:
                                    if problem_refs_lookup[job_id][ws_obj_id] is None:
                                        # Do nothing can not resolve the correct id
                                        continue
                                    else:
                                        used_ws_obj_id =  problem_refs_lookup[job_id][ws_obj_id]
                                        needs_to_be_added_to_the_db = 0
                                else:
                                    # THE incomplete Reference needs to be tried to be resolved and then inserted into the DB
                                    if len(elements) == 2:
                                        has_2_elements_count += 1
                                        #                                    print("in elements == 2")
                                        ws_id = None
                                        obj_id = None
                                        if elements[0].isdigit():
                                            # the ws_id is a number. it is good to go
                                            ws_id = int(elements[0])
                                        else:
                                            # means the ws is identified by name and not by id
                                            # Need to search the worksaceObjects table to get the id.
                                            # Note there is no mechanism for users to change this value
                                            # There are no dupicate named workspaces other than null (which has 2)
                                            workspaces_cursor = db.workspaces.find({"name":elements[0]},{"ws":1});
                                            for record in workspaces_cursor:
                                                ws_id = int(record["ws"])
#                                        print("ws_id resolved:  " + str(ws_id))
                                        if elements[1].isdigit():
                                            obj_id = int(elements[1])
                                        else:
#                                            print("IN resolve object name")
                                            # means the obj portion of the reference is identified by a name
                                            # NOTE THIS NAME CAN BE CHANGED BY THE USER
                                            # IF THE USER CHANGED THE NAME SINCE THE TIME OF THE DOWNLOAD
                                            # THEN THAT REFERENCE IS ORPHANED
                                            # Need to query the workspaceOBjects mongo collection
                                            # using the name and ws_id to determine the object id
                                            workspaceObjects_cursor = db.workspaceObjects.find({"name":elements[1],"ws":ws_id},{"id":1});
#                                           print("elements[1] = " + elements[1])
#                                           print("ws id : " + str(ws_id))
#                                           print("workspaceObjects_cursor" + str(workspaceObjects_cursor))
                                            for record in workspaceObjects_cursor:
#                                            print("Found wsObjects record : " + str(record) ) 
                                                obj_id = int(record["id"])

#                                    print("ws_obj_id : " + ws_obj_id + " resolved to : " + str(obj_id))

                                        if obj_id is not None and ws_id is not None:
                                             # Need to do time machine to determine which object version was active
                                             # at the time of the Downloading job start time
#                                            print("Found input ref: " + ws_obj_id)
                                             job_start_epoch = job["running"] / 1000
#                                            print("job_start_epoch : " + str(job_start_epoch))
                                             max_save_date_epoch = 0
                                             max_save_date_version = 0
                                             workspaceObjVersions_cursor = db.workspaceObjVersions.find({"ws": int(ws_id), "id": int(obj_id)},
                                                                                                   {"ws": 1, "id": 1, "ver": 1, "savedate": 1, "_id": 0})
                                             for record in workspaceObjVersions_cursor:
                                                 iso_savedate = record["savedate"]
                                                 iso_savedate_string = str(iso_savedate)
                                                 iso_savedate_string_elements = iso_savedate_string.split(".")
                                                 if len(iso_savedate_string_elements) == 1:
                                                     iso_savedate_string = iso_savedate_string + ".000000"
                                                 utc_dt = datetime.strptime(iso_savedate_string,'%Y-%m-%d %H:%M:%S.%f')
                                                                           #'%Y-%m-%dT%H:%M:%S.%fZ')
                                                 savedate_epoch = (utc_dt - datetime(1970, 1, 1)).total_seconds()
#                                                 print("savedate_epoch : " + str(savedate_epoch))
                                                 if (job_start_epoch > savedate_epoch and savedate_epoch > max_save_date_epoch):
                                                     max_save_date_epoch = savedate_epoch
                                                     max_save_date_version = record["ver"]
#                                            if (max_save_date_version > 1):
#                                                 print("FINAL VERSION saved : " + str(max_save_date_version))
                                             used_ws_obj_id = str(ws_id) + "/" + str(obj_id) + "/" + str(max_save_date_version)
#                                             print("used_ws_obj_id : " + used_ws_obj_id)
                                        else:
                                            # One of the ws_id or obj_id is None most likely means orphaned reference due to
                                            #object name change
                                            used_ws_obj_id = None
                                            downloading_jobs_with_orphaned_refs_count += 1
                                    else:
                                        print("WS OBJ ID was a different format then expected")
                                        used_ws_obj_id = None
                                        downloading_jobs_with_orphaned_refs_count += 1
                                # END OF TRYING TO DETERMINE FULL WS_OBJ_ID

                                # ENTER RECORD INTO DOWNLOADER_RESULTS
                                if used_ws_obj_id not in downloader_results:
                                    downloader_results[used_ws_obj_id] = dict()
                                if username not in downloader_results[used_ws_obj_id]:
                                    downloader_results[used_ws_obj_id][username] = list()
                                downloader_results[used_ws_obj_id][username].append(job_id)

                                if needs_to_be_added_to_the_db == 1 :
                                    #need to do insert
                                    input = (ws_obj_id, used_ws_obj_id, job_id)
                                    prep_cursor.execute(downloaders_problematic_obj_ids_insert_statement, input)
                                    insert_prob_refs_count += 1

                                #downloader_results[ws_obj_id][job_id] = username
#                                downloader_results[used_ws_obj_id] +=1
                                #downloader_results[ws_obj_id].add(username)
                                downloaders_with_ws_id_count += 1
#                                if example_counter < 10:
#                                    print("Example input_ws_obj_id : " + ws_obj_id + " resolved to used_ws_obj_id : " + used_ws_obj_id)
#                                    example_counter += 1
#                                else:
#                                    print("EARLY EXIT: DOWNLOADER RESULTS : " + str(downloader_results))
#                                    exit()
                            else:
                                download_job_without_input_ref_count += 1
                                downloaders_dict[method]["no_input_ref_count"] += 1
                    finished_job_count += 1
                    yearly_finished_count += 1
            print("Yearly downloader_count : " + str(yearly_downloader_count))
            print("Yearly finished_count : " + str(yearly_finished_count))
            print("Yearly download_job_without_input_ref_count : " + str(download_job_without_input_ref_count))
            print("Year to do end: " + str(year_to_do) + "_" + str(part_of_year) + " :: " + str(time.time() - yearly_start_time) + " seconds")
            print("kbObjectInfo_dict : " + str(kbObjectInfo_dict))
            print("kbObjectInfo_dict len : " + str(len(kbObjectInfo_dict)))


    print(str(downloaders_dict))

#    i = 0
#    while i < 3:
#        print("DataFileUtil_download_web_file_jobs number : " + str(i))
#        print(DataFileUtil_download_web_file_jobs[i])
#        i += 1

#    i = 0
#    while i < 3:
#        print("fba_tools_bulk_export_objects_jobs number : " + str(i))
#        print(fba_tools_bulk_export_objects_jobs[i])
#        i += 1

#    i = -10
#    while i < 0: 
#        print("fba_tools_bulk_export_objects_jobs number : " + str(i))
#        print(fba_tools_bulk_export_objects_jobs[i])
#        i += 1


    print("TOTAL length of fba_tools_bulk_export_objects_jobs : " + str(len(fba_tools_bulk_export_objects_jobs)))
    print("counter : " + str(fba_tools_bulk_export_objects_job_count))
#    print("DOWNLOADER RESULTS:")
#    print(str(downloader_results))
#    loop_count = 0
    db_connection.commit()
    print("Finished job count : "  + str(finished_job_count))
    print("In If count : "  + str(in_if_count))
    print("Downloaders job count : "  + str(downloaders_count))

    print("Downloaders with ws_id count : "  + str(downloaders_with_ws_id_count))
    print("Has 2 elements count : has_2_elements_count : " + str(has_2_elements_count))
    print("FINAL DOWNLADER METHODS WITH AND WITHOUT INPUT REFS : ")
    
    print("insert_prob_refs_count :  " + str(insert_prob_refs_count))

    return downloader_results

def get_downloaders_lookup():

    start_time = time.time()    
    main_function_start_time = time.time()
    
    downloaders_set = get_downloaders_set(cursor)
    problem_refs_lookup = get_existing_problem_refs(cursor)
    downloader_results = pull_downloading_jobs(downloaders_set, problem_refs_lookup)
    print("--- Total TIME for building downloading lookups %s seconds ---" % (time.time() - start_time))
    return downloader_results



#downloader_results = get_downloaders_lookup()
#i = 0
#for downloader_key in downloader_results:
#    print("Downloader key : " + str(downloader_key) + " downloader_results : " + str(downloader_results[downloader_key]))
#    if i > 10:
#        break
#    i = i + 1
#print("Downloader_results : " + str(downloader_results['49114/8/1']))    



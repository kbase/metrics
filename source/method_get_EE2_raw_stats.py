# GetEE2AppStats
# Mimics output from old catalog function 'get_exec_raw_stats'

import biokbase.narrative.clients as clients
import datetime

ee2 = clients.get('execution_engine2')
# Insures all finish times within last day.
yesterday = (datetime.date.today() - datetime.timedelta(days=1))


def get_user_app_stats(start_date=datetime.datetime.combine(yesterday, datetime.datetime.min.time()),
                       end_date=datetime.datetime.combine(yesterday, datetime.datetime.max.time())):
    """
    Gets a data dump from EE2 for a certain date window.
    If no start and end date are entered it will default to the last 15 calendar days (UTC TIME).
    It is 15 days because it uses an underlying method that
    filters by creation_time and not finish_time
    """
    # From str to datetime, defaults to zero time.
    if type(start_date) == str:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    # Due to issue with method filtering only by creation_time need to grab
    # all 14 days before begin date to insure getting all records with a possible
    # finish_time within the time window specified. (14 days, 24 hours, 60 mins, 60 secs)
    begin = (int(start_date.strftime('%s')) - (14 * 24 * 60 * 60)) * 1000
    end = int(end_date.strftime('%s')) * 1000
    statuses = ['queued', 'terminated', 'running', 'created', 'estimated']
    job_array = []
    # print("BEGIN: " + str(begin))
    # print("END: " + str(end))
    # For params get finished jobs from execution engine
    params = {'start_time': begin, 'end_time': end, 'ascending': 0}
    stats = ee2.check_jobs_date_range_for_all(params=params)
    for job in stats['jobs']:
        if job['status'] in statuses:
            continue
        else:
            # For finished job run calculate run time and convert values from milliseconds to seconds
            run_time = (job['finished'] - job['running'])
            finished = datetime.datetime.fromtimestamp(job['finished'] / 1000)
            run_start = datetime.datetime.fromtimestamp(job['running'] / 1000)
            is_error = "False"
            if 'error' in job:
                is_error = "True"
            # For values present construct job stats dictionary and append to job array
            job_stats = {'user': job['user'],
                         'finish_date': finished.strftime('%Y-%m-%d %H:%M:%S'),
                         'start_date': run_start.strftime('%Y-%m-%d %H:%M:%S'),
                         'run_time': run_time//1000,
                         'app_name': job['job_input']['app_id'].replace('.', '/'),
                         'func_name': job['job_input']['method'].replace('.', '/'),
                         "git_commit_hash": job['job_input']['service_ver'],
                         'is_error': is_error
                         }
            job_array.append(job_stats)
    return job_array

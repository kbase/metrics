

import requests
requests.packages.urllib3.disable_warnings()
from biokbase.catalog.Client import Catalog
from biokbase.narrative_method_store.client import NarrativeMethodStore
from category_to_app_dict import create_app_dictionary_1
catalog = Catalog(url = "https://kbase.us/services/catalog")
nms = NarrativeMethodStore(url = "https://kbase.us/services/narrative_method_store/rpc")

import pandas as pd
import datetime, time
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
from operator import itemgetter


# Get User_App_Stats: Main Function
"""Requires a unique list of username as input and runs it 3-4min on average.
outputs a list of user dictionaries with app statistics for each userm example user : [{app1_usage_info}, {app2_usage_info}]"""


# Get User_App_Stats

def user_app_stats(unique_usernames, start_date=datetime.datetime.now() - datetime.timedelta(days=1),
                   end_date=datetime.datetime.now()):

    now = int(time.time())
    time_interval = {'begin': 1, 'end': now}

    stats = catalog.get_exec_raw_stats(time_interval)
    stats = sorted(stats, key=itemgetter('exec_start_time'), reverse=True)
    catalog_data_all = pd.DataFrame.from_dict(stats)

    catalog_data_all.creation_time = pd.to_datetime(catalog_data_all['creation_time'], unit='s')
    catalog_data_all.exec_start_time = pd.to_datetime(catalog_data_all['exec_start_time'], unit='s')
    catalog_data_all.finish_time = pd.to_datetime(catalog_data_all['finish_time'], unit='s')
    catalog_data_all.drop(['git_commit_hash', 'app_module_name', 'func_module_name', 'func_name', 'job_id'], axis=1,
                          inplace=True)
    catalog_data_all = catalog_data_all.assign(run_time=(catalog_data_all['finish_time'] -
                                                         catalog_data_all['exec_start_time']).astype('timedelta64[s]'))

    # Slice catalog data by data range
    catalog_data_all.index = catalog_data_all['creation_time']
    catalog_data_all.sort_index(inplace=True)
    catalog_data = catalog_data_all[start_date:end_date]

    # Filter null values in app_id
    catalog_data["app_id"].fillna("Not Specified", inplace=True)
    catalog_data = catalog_data[catalog_data.app_id != "Not Specified"]
    # Initiate dictionaries and arrays

    # Need app_dict here!
    app_dict = create_app_dictionary_1()
    total_user_app_stats = []
    values = app_dict.values()
    KBase_apps = [item for sublist in values for item in sublist]

    # Iterate over all KBase users
    for user in unique_usernames:
        # Get catalog data for specific user
        user_condition = catalog_data.user_id == user
        user_specific_catdata = catalog_data[user_condition]
        # Get all apps used by user
        app_lst = list(set(list(user_specific_catdata.app_id)))

        if not app_lst:
            continue

        user_app_dict_lst = []

        # for app in user's app list get stats
        for app in app_lst:
            # if app name in app dictionary get category of app, make app_stats dict and append dict to users app dictionary
            if app in KBase_apps:

                # Initiate app and error condition
                app_condition = user_specific_catdata.app_id == app
                # Count app usage and errors
                catalog_at_app = user_specific_catdata[app_condition]

                for i in catalog_at_app.index.values:
                    start_date = catalog_at_app.exec_start_time[i]
                    finish_date = catalog_at_app.finish_time[i]
                    run_time = catalog_at_app.run_time[i]
                    is_error = catalog_at_app.is_error[i]

                    # Make app_stats dictionary for app and append to array
                    app_stats = {"user_name": user, "app_name": app, "start_date": start_date,
                                 "finish_date": finish_date, "run_time": run_time, "is_error": is_error}
                    user_app_dict_lst.append(app_stats)

        # user_app_dict_lst = [item for sublist in user_app_dict_lst for item in sublist]
        # print(user_app_dict_lst)

        total_user_app_stats.extend(user_app_dict_lst)

    return total_user_app_stats

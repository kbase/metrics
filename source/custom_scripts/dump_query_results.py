#!/usr/local/bin/python

import os
import mysql.connector as mysql

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
sql_host = os.environ["SQL_HOST"]
metrics = os.environ["QUERY_ON"]


def dump_query_results():
    """
    It is a simple SQL table dump of a given query so we can supply users with custom tables.
    Note that the SQL query itself and column headers portion need to be changed if you want to change
    the query/results. Otherwise it is good to go.
    It can be called simply with the bin shell script.
    Read the README at the top level for an example.
    docker-compose run --rm metrics ../bin/custom_scripts/dump_query_results.sh > query_results.txt

    """
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host,  # "mysql1", #"localhost",
        user="metrics",  # "root",
        passwd=metrics_mysql_password,
        database="metrics",  # "datacamp"
    )

    cursor = db_connection.cursor()
    query = "use " + metrics
    cursor.execute(query)

    # CHANGE QUERIES AND HEADERS HERE

    # USER SUPER SUMMARY
    query = ("select * from metrics_reporting.user_super_summary")    
    #    HEADERS FOR user_super_summary
    print(
        "username\tdisplay_name\temail\tkb_internal_user\tuser_id\tglobus_login\tgoogle_login\torcid\tsession_info_country\tcountry\tstate\t"
        "institution\tdepartment\tjob_title\thow_u_hear_selected\thow_u_hear_other\tdev_token_first_seen\tsignup_date\tlast_signin_date\tdays_signin_minus_signup\t"
        "days_since_last_signin\tnum_orgs\tnarrative_count\tshared_count\tnarratives_shared\tfirst_narrative_made_date\tlast_narrative_made_date\t"
        "last_narrative_modified_date\ttotal_narrative_objects_count\ttop_lvl_narrative_objects_count\ttotal_narrative_objects_size\t"
        "top_lvl_narrative_objects_size\ttotal_narrative_count\ttotal_public_narrative_count\tdistinct_static_narratives_count\t"
        "static_narratives_created_count\ttotal_visible_app_cells\ttotal_code_cells_count\tfirst_file_date\tlast_file_date\t"
        "total_file_sizes_MB\ttotal_file_count\tmost_used_app\tdistinct_apps_used\ttotal_apps_run_all_time\ttotal_apps_run_last365\t"
        "total_apps_run_last90\ttotal_apps_run_last30\ttotal_app_errors_all_time\tfirst_app_run\tlast_app_run\ttotal_run_time_hours\t"
        "total_queue_time_hours\ttotal_CPU_hours\tsession_count_all_time\tsession_count_last_year\tsession_count_last_90\tsession_count_last_30"
    )

    # APP FLOWS - for Adam's narrative_app_flow
    #query = ("select * from metrics_reporting.narrative_app_flows")
    #print("ws_id\tusername\tapp_name\tfunc_name\tstart_date\tfinish_date") 
    
    # app popularity growth
    #query = ("select uau.app_name, DATE_FORMAT(`finish_date`,'%Y-%m') as run_month, count(*) as run_count, sum(run_time)/3600 as total_run_hours\
    #          from metrics.user_app_usage uau inner join metrics.user_info ui on uau.username = ui.username\
    #          where ui.kb_internal_user = 0\
    #          group by uau.app_name, run_month\
    #          order by run_month, app_name")
    #print("app_name\trun_month\trun_count\ttotal_run_hours")

    # App category run totals
    #query = ("select uau.app_name,\
    #         IFNULL(app_category, \"No Category Association\") as app_cat,\
    #         DATE_FORMAT(`finish_date`,'%Y-%m') as run_month, count(*) as run_count,\
    #         sum(run_time)/3600 as total_run_hours\
    #         from metrics.user_app_usage uau inner join\
    #         metrics.user_info ui on uau.username = ui.username\
    #         left outer join\
    #         metrics.app_name_category_map anm on uau.app_name = anm.app_name\
    #         where ui.kb_internal_user = 0\
    #         group by uau.app_name, app_cat, run_month\
    #         order by run_month, app_name;")
    #print("app_name\tapp_cat\trun_month\trun_count\ttotal_run_hours")

    # USER SESSION STATS:
    #query = ("select si.username, count(*) as session_count, sum(estimated_hrs_active) total_hours_active,\
    #          avg(estimated_hrs_active) avg_hours_active, std(estimated_hrs_active) std_hours_active,\
    #          min(first_seen), max(last_seen)\
    #          from metrics.user_info ui inner join metrics.session_info si on ui.username = si.username\
    #          where estimated_hrs_active < 24\
    #          group by username\
    #          order by avg_hours_active desc, session_count, total_hours_active")
    #print("username\tsession_count\ttotal_hours_active\tavg_hours_active\tstd_hours_active\tfirst_seen\tlast_seen")
    
    cursor.execute(query)
    row_values = list()

    for row_values in cursor:
        temp_string = ""
        for i in range(len(row_values) - 1):
            if row_values[i] is not None:
                temp_string += str(row_values[i])
            temp_string += "\t"
        if row_values[-1] is not None:
            temp_string += str(row_values[-1])
        print(temp_string)
    return 1


dump_query_results()

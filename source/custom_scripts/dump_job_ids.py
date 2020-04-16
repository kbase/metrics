#!/usr/local/bin/python

import os
import mysql.connector as mysql    

metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']
sql_host = os.environ['SQL_HOST']
metrics = os.environ['QUERY_ON']

def dump_query_results():
    """ 
    This is a simple SQL table dump of a given query so we can supply users with custom tables.
    Note that the SQL query itself and column headers portion need to be changed if you want to change 
    the query/results. Otherwise it is good to go. 
    It can be called simply with the bin shell script. 
    Read the README at the top level for an example.
    """
    #connect to mysql
    db_connection = mysql.connect(
        host = sql_host,#"mysql1", #"localhost",
        user = "metrics", #"root",
        passwd = metrics_mysql_password,
        database = "metrics" #"datacamp"
    )

    cursor = db_connection.cursor()
    query = "use "+metrics
    cursor.execute(query)

    #CHANGE QUERY HERE
    query = "select job_id from user_app_usage where finish_date <= '2020-03-03 07:05:15' and job_id not in (select job_id from user_app_usage_ee2);";
#    query = "select func_name, DATE_FORMAT(`finish_date`,'%Y-%m') as finish_month, count(*) as run_count, "\
#            "avg(run_time) as avg_run_time_secs, sum(run_time) as total_run_time_secs "\
#            "from metrics.user_app_usage where is_error = 0 group by func_name, finish_month;"
    #CHANGE COLUMN HEADERS HERE TO MATCH QUERY HEADERS
#    print("username\temail\tlast_signin_date\tmax_last_seen\tHasBeenSeen")
#    print("ws_id\tusername\tmod_date\tinitial_save_date\trecord_date\ttop_lvl_object_count\ttotal_object_count\tvisible_app_cells_count\tnarrative_version\thidden_object_count\tdeleted_object_count\ttotal_size\ttop_lvl_size\tis_public\tis_temporary\tnumber_of_shares")
#    print("function_name\tfinish_month\tsuccessful_run_count\taverage_run_time\ttotal_run_time")

    cursor.execute(query)
    row_values = list()

    for (row_values) in cursor:
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


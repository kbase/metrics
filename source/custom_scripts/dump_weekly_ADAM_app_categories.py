#!/usr/local/bin/python

import os
import mysql.connector as mysql

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
sql_host = os.environ["SQL_HOST"]
metrics = os.environ["QUERY_ON"]


def dump_weekly_app_categories():
    # Dumps the weekly app catagory users report used in the quarterly report

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

    # CHANGE QUERY HERE
    query = ("select * from metrics_reporting.app_category_unique_users_weekly")
    query = ("select in_query.week_run, in_query.master_category, count(*) as unique_users "
             "from (select distinct DATE_FORMAT(`finish_date`,'%Y-%u') as week_run, "
             "IFNULL(master_category,'None') as master_category, uau.username "
             "from metrics.user_app_usage uau inner join "
             "metrics.user_info ui on uau.username = ui.username "
             "left outer join "
             "metrics.adams_app_name_category_map anc on uau.app_name = anc.app_name "
             "where ui.kb_internal_user = 0 "
             "and func_name != 'kb_gtdbtk/run_kb_gtdbtk') as in_query "
             "group by in_query.week_run, in_query.master_category;")
    # CHANGE COLUMN HEADERS HERE TO MATCH QUERY HEADERS
    print("week_run\tmaster_category\tunique_users")

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


dump_weekly_app_categories()

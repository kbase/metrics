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
    query = ("select * from metrics_reporting.app_category_unique_users_weekly_v2")
    # CHANGE COLUMN HEADERS HERE TO MATCH QUERY HEADERS
    print("week_run\tapp_category\tunique_users")

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

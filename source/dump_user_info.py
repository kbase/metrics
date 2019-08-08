#!/usr/local/bin/python

import os
metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']


def dump_user_info():
    import mysql.connector as mysql    

    #connect to mysql
    db_connection = mysql.connect(
        host = "10.58.0.98",#"mysql1", #"localhost",
        user = "metrics", #"root",
        passwd = metrics_mysql_password,
        database = "metrics" #"datacamp"
    )

    cursor = db_connection.cursor()
    query = "use metrics"
    cursor.execute(query)

    query = "select username, display_name, email, orcid, kb_internal_user, institution, country, signup_date, last_signin_date from user_info"
    cursor.execute(query)
    row_values = list()
    print("username, display_name, email, orcid, kb_internal_user, institution, country, signup_date, last_signin_date")
    for (row_values) in cursor:
        temp_string = ""
        for i in range(8):
            if row_values[i] is not None:
                temp_string += str(row_values[i])
            temp_string += ","
        if row_values[8] is not None:
            temp_string += str(row_values[i])
        print(temp_string)
    return 1

dump_user_info()


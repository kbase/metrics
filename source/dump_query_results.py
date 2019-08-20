#!/usr/local/bin/python

import os
metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']


def dump_query_results():
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

    #CHANGE QUERY HERE
    query = "select username, display_name, email, orcid, kb_internal_user, institution, country, signup_date, last_signin_date from user_info order by signup_date"
    #CHANGE COLUMN HEADERS HERE TO MATCH QUERY HEADERS
    print("username\tdisplay_name\temail\torcid\tkb_internal_user\tinstitution\tcountry\tsignup_date\tlast_signin_date")

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


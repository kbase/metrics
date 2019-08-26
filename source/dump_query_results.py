#!/usr/local/bin/python

import os
metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']


def dump_query_results():
    """ 
    This is a simple SQL table dump of a given query so we can supply users with custom tables.
    Note that the SQL query itself and column headers portion need to be changed if you want to change 
    the query/results. Otherwise it is good to go. 
    It can be called simply with the bin shell script. 
    Read the README at the top level for an example.
    """
    import mysql.connector as mysql    
    sql_host = os.environ['SQL_HOST']
    #connect to mysql
    db_connection = mysql.connect(
        host = sql_host,#"mysql1", #"localhost",
        user = "metrics", #"root",
        passwd = metrics_mysql_password,
        database = "metrics" #"datacamp"
    )

    cursor = db_connection.cursor()
    query = "use"+os.environ['QUERY_ON']
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


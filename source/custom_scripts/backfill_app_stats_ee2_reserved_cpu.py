import os

import mysql.connector as mysql

sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]
metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]


def make_lookup_dict():
    cpu_file = open("custom_scripts/app_reserved_cpu_lookup_file.txt", "r")
    lines = cpu_file.readlines()

    count = 0
    reserved_cpu_lookup_dict = dict()
    for line in lines:
        elements = line.strip().split("\t")
        func_name = elements[0] + "/" + elements[1]
        parts = elements[2].split()
        for part in parts:
            if part.startswith("request_cpus="):
                sub_parts = part.split("=")
                reserved_cpu_lookup_dict[func_name] = int(sub_parts[1])
    #    print(str(reserved_cpu_lookup_dict))
    return reserved_cpu_lookup_dict


def backfill_reserved_cpu(reserved_cpu_lookup_dict):
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    distinct_apps_list = list()
    get_distinct_list_of_apps_q = (
        "select distinct func_name "
        "from user_app_usage_ee2_cpu "
        "where reserved_cpu is null"
    )

    cursor.execute(get_distinct_list_of_apps_q)
    for row in cursor:
        distinct_apps_list.append(row[0])
    #    print(str(distinct_apps_list))

    reserved_cpu_update_prep_cursor = db_connection.cursor(prepared=True)
    reserved_cpu_update_stmt = (
        "update metrics.user_app_usage_ee2_cpu set reserved_cpu = %s "
        "where func_name = %s and reserved_cpu is null;"
    )

    unfound_counter = 0
    found_counter = 0
    reserved_cpu = 4
    for app in distinct_apps_list:
        if app in reserved_cpu_lookup_dict:
            found_counter += 1
            reserved_cpu = reserved_cpu_lookup_dict[app]
        else:
            unfound_counter += 1
        input = [reserved_cpu, app]
        reserved_cpu_update_prep_cursor.execute(reserved_cpu_update_stmt, input)
    print("FOUND : " + str(found_counter))
    print("UNFOUND : " + str(unfound_counter))
    db_connection.commit()
    return 1


reserved_cpu_lookup_dict = make_lookup_dict()
backfill_reserved_cpu(reserved_cpu_lookup_dict)

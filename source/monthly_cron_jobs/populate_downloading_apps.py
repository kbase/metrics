import os
import mysql.connector as mysql
from biokbase.narrative_method_store.client import NarrativeMethodStore

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]


def get_downloader_apps():
    nms = NarrativeMethodStore(url=os.environ["NARRATIVE_METHOD_STORE"])
    list_types = nms.list_types({})
    downloader_methods = set()
    for type_dict in list_types:
        if "export_functions" in type_dict:
            if len(type_dict["export_functions"]) > 0:
                for download_type in type_dict["export_functions"]:
                    downloader_methods.add(type_dict["export_functions"][download_type])
    print("Downloader Methods Set")
    print(str(downloader_methods))
    return downloader_methods

def upload_downloader_apps():
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    # get existing mappings
    existing_downloaders_list = list()
    query = "select downloader_app_name from metrics.downloader_apps"
    cursor.execute(query)
    for row in cursor:
        existing_downloaders_list.append(row[0])

    existing_count = len(existing_downloaders_list)
    # insert statement
    insert_prep_cursor = db_connection.cursor(prepared=True)
    insert_statement = (
        "insert into metrics.downloader_apps "
        "(downloader_app_name) "
        "values(%s);"
    )
    insert_count = 0

    current_downloader_apps = get_downloader_apps()

    for current_downloader_app in current_downloader_apps:
        if current_downloader_app not in existing_downloaders_list:
            input_args = (current_downloader_app,)
            insert_prep_cursor.execute(insert_statement, input_args)
            insert_count += 1
    db_connection.commit()
    print("Existing_count : " + str(existing_count))
    print("Insert_count : " + str(insert_count))


upload_downloader_apps()

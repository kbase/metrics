from pymongo import MongoClient
from pymongo import ReadPreference
import os
import mysql.connector as mysql
import time
import datetime
metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]

sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]

def upload_user_orcid_record_link_count():
    """
    Populates the table with the public narrative count
    """
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    user_orcid_record_link_count = None
    select_query =  (
        "select count(*) from metrics_reporting.user_info_summary_stats "
        "where kb_internal_user = 0 and orcid_record_link is not null"
    )
    cursor.execute(select_query)
    for row in cursor:
        user_orcid_record_link_count = row[0]

    print("COUNT RETRIEVED: " + str(user_orcid_record_link_count))
    insert_cursor = db_connection.cursor(prepared=True)
    oidc_insert_statement = (
        "insert into metrics.user_orcid_record_link_count "
        "(user_orcid_record_link_count) values(%s)"
    )
    input_vals = (user_orcid_record_link_count,)
    insert_cursor.execute(oidc_insert_statement, input_vals)
    db_connection.commit()
    print("User ORCID Record Link Count : " + str(user_orcid_record_link_count))
    return

print("############################################")
print("USER ORCID Record Link count Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
upload_user_orcid_record_link_count()
print("Finished: " + str(datetime.datetime.utcnow()))
print("--- USER ORCID Record Link count time :  %s seconds ---" % (time.time() - start_time))
print("############################################")

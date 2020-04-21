from pymongo import MongoClient
from pymongo import ReadPreference
import os
import mysql.connector as mysql

metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']
mongoDB_metrics_connection = os.environ['MONGO_PATH']

sql_host = os.environ['SQL_HOST']
query_on = os.environ['QUERY_ON']

to_workspace =  os.environ['WRK_SUFFIX']

def get_public_narrative_count():
    """
    Gets the number of pu;ic_narratives
    """
    client_workspace = MongoClient(mongoDB_metrics_connection+to_workspace)
    db_workspace = client_workspace.workspace
    public_narrative_count = db_workspace.workspaceACLs.find({"user" : "*"}).count()
    return public_narrative_count;


def upload_public_narratives_count():
    '''
    Populates the table with the public narrative count
    '''
    #connect to mysql
    db_connection = mysql.connect(
        host = sql_host,
        user = "metrics",
        passwd = metrics_mysql_password,
        database = "metrics"
    )
    cursor = db_connection.cursor()
    query = "use "+query_on
    cursor.execute(query)

    insert_cursor = db_connection.cursor(prepared=True)
    pnc_insert_statement = "insert into metrics.public_narrative_count " \
                           "(public_narrative_count) values(%s)"
    pnc = get_public_narrative_count()
    input_vals = (pnc,)
    insert_cursor.execute(pnc_insert_statement,input_vals)
    db_connection.commit()
    print("Public Narratives Count : " + str(pnc))
    return



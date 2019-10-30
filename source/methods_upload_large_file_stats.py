from pymongo import MongoClient
from pymongo import ReadPreference
import os
import mysql.connector as mysql
import datetime, time


metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']
mongoDB_metrics_connection = os.environ['MONGO_PATH']
mongoDB_metricsro_connection = os.environ['METRICSRO_MONGO_PATH']

sql_host = os.environ['SQL_HOST']
query_on = os.environ['QUERY_ON']

to_workspace =  os.environ['WRK_SUFFIX']
to_shock =  os.environ['SHOCK_SUFFIX']

#Insures all finish times within last day.
yesterday = (datetime.date.today() - datetime.timedelta(days=1))

def get_uuid_user_mappings(db_shock):
    '''
    Get the uuid to users mappings
    '''
    uuid_user_mapping_dict = dict()
    user_query = db_shock.Users.find({},{"_id":0,"username":1,"uuid":1})
    for record in user_query:
        uuid_user_mapping_dict[record["uuid"]]=record["username"]
#    print("DICT"+str(uuid_user_mapping_dict))
    print("uuid_user+mapping_length : " + str(len(uuid_user_mapping_dict)))
    return uuid_user_mapping_dict

def get_nodes_data(db_shock,uuid_user_mapping_dict,start_date,end_date):
    '''
    Get the Nodes data : uuid(map later to username), creation_date, size
    '''
    #Dict {username->{date => {count => number, total_size => number}}}
    users_date_summary_dict = dict()
    nodes_query = db_shock.Nodes.find({"created_on": {"$gt":start_date,"$lt":end_date}},{"_id":0,"acl.owner":1,"created_on":1,"file.size":1})
    total_nodes_count = 0
    for record in nodes_query:
        created_on = record["created_on"]
        record_date = created_on.date()
        username = uuid_user_mapping_dict[record["acl"]["owner"]]
        size = record["file"]["size"]
        if username not in users_date_summary_dict:
            users_date_summary_dict[username] = dict()
        if record_date not in users_date_summary_dict[username]:
            users_date_summary_dict[username][record_date] = {"total_size":size,"count":1}
        else:
            users_date_summary_dict[username][record_date]["total_size"] += size
            users_date_summary_dict[username][record_date]["count"] += 1
        print("record_date: " + str(record_date))
        print("username: " + str(username))
        print("size: " + str(size))
        total_nodes_count += 1
    print("TOTAL NODES COUNT:" + str(total_nodes_count))
    return users_date_summary_dict


def process_large_file_data(start_date=datetime.datetime.combine(yesterday, datetime.datetime.min.time()),
                       end_date=datetime.datetime.combine(yesterday, datetime.datetime.max.time()) ):
    '''
    Get the large file stats (historically SHOCK)
    '''

    #get mongo set up
    client_shock = MongoClient(mongoDB_metricsro_connection+to_shock)
    db_shock = client_shock.ShockDB

    # From str to datetime, defaults to zero time.
    if type(start_date) == str:
        start_date_partial = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        start_date=datetime.datetime.combine(start_date_partial, datetime.datetime.min.time())
        end_date_partial = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        end_date=datetime.datetime.combine(end_date_partial, datetime.datetime.max.time())
    
    print("ISO Start Date: " + str(start_date.isoformat()))
    print("ISO End Date: " + str(end_date.isoformat()))

    print("Start Date: " + str(start_date))
    print("End Date: " + str(end_date))

    uuid_user_mapping_dict = get_uuid_user_mappings(db_shock)
    users_date_summary_dict = get_nodes_data(db_shock,uuid_user_mapping_dict,start_date,end_date)

    print("users date dict:" + str(users_date_summary_dict))
    return users_date_summary_dict





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


#get_large_file_data()
start_date = "2019-10-28"
end_date = "2019-10-31"
process_large_file_data(start_date,end_date)

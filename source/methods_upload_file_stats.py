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
    #print("DICT"+str(uuid_user_mapping_dict))
    #print("uuid_user+mapping_length : " + str(len(uuid_user_mapping_dict)))
    return uuid_user_mapping_dict

def get_nodes_data(db_shock,uuid_user_mapping_dict,start_date,end_date):
    '''
    Get the Nodes data : uuid(map later to username), creation_date, size
    '''
    #Dict {username->{date => {count => number, total_size => number}}}
    users_date_summary_dict = dict()
    nodes_query = db_shock.Nodes.find({"created_on": {"$gt":start_date,"$lt":end_date}},{"_id":0,"acl.owner":1,"created_on":1,"file.size":1})
    total_nodes_count = 0
    records_missing_acl_owner = 0
    records_acl_owner_not_in_users = 0
    for record in nodes_query:
        created_on = record["created_on"]
        record_date = created_on.date()
        if "acl" in record and "owner" in record["acl"]:
            if record["acl"]["owner"] == "public":
                username = "public"
            elif record["acl"]["owner"] not in uuid_user_mapping_dict:
                records_acl_owner_not_in_users+=1
                continue
            else:
                username = uuid_user_mapping_dict[record["acl"]["owner"]]
        else:
            records_missing_acl_owner+=1
            continue
        size = record["file"]["size"]
        if username not in users_date_summary_dict:
            users_date_summary_dict[username] = dict()
        if record_date not in users_date_summary_dict[username]:
            users_date_summary_dict[username][record_date] = {"total_size":size,"file_count":1}
        else:
            users_date_summary_dict[username][record_date]["total_size"] += size
            users_date_summary_dict[username][record_date]["file_count"] += 1
        #print("record_date: " + str(record_date))
        #print("username: " + str(username))
        #print("size: " + str(size))
        total_nodes_count += 1
    #print("TOTAL NODES COUNT:" + str(total_nodes_count))
    if records_missing_acl_owner > 0:
        print("Records missing ACL owners:" + str(records_missing_acl_owner))
    if records_acl_owner_not_in_users > 0:
        print("Records ACL owners not in users:" + str(records_acl_owner_not_in_users))
    return users_date_summary_dict

def upload_file_stats_data(users_date_summary_dict):
    '''
    Populates the table file_storage_stats
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
    file_stats_insert_statement = "insert into metrics.file_storage_stats " \
                                  "(username, record_date, total_size, file_count) values(%s, %s, %s, %s)"
    check_record_cursor = db_connection.cursor(buffered=True)
    check_record_statement = "select total_size, file_count "\
                             "from metrics.file_storage_stats " \
                             "where username = %s and record_date = %s"
    update_cursor = db_connection.cursor(prepared=True)
    file_stats_update_statement = "update metrics.file_storage_stats " \
                                  "set username = %s, record_date = %s, " \
                                  "total_size = %s, file_count = %s " \
                                  "where username = %s and record_date = %s;"

    duplicates_updated_count = 0
    duplicates_skipped_count = 0
    fail_inserts_count = 0
    insert_count = 0
    for username in users_date_summary_dict:
        for record_date in users_date_summary_dict[username]:
            input_vals = (username, record_date, users_date_summary_dict[username][record_date]["total_size"],
                          users_date_summary_dict[username][record_date]["file_count"])
            try:
                insert_cursor.execute(file_stats_insert_statement,input_vals)
                insert_count += 1
            except mysql.Error as err:
                #There can be two types of errors : 
                # 1) duplicate ysername/record_date combo - 
                #      May need to update the record
                # 2) missing username in userinfo if update user_stats have not been run recently
                try:
                    check_vals = (username, record_date)
                    check_record_cursor.execute(check_record_statement,check_vals)
                except mysql.Error as err2:
                    print("ERROR2: " + str(err2))
                    print("ERROR2 Input: " + str(check_vals))
                    exit
                if check_record_cursor.rowcount > 0:
                    # means the record exists
                    for (temp_total_size, temp_file_count) in check_record_cursor:
                        # see if it needs to be update
                        if (temp_total_size == users_date_summary_dict[username][record_date]["total_size"] and
                            temp_file_count == users_date_summary_dict[username][record_date]["file_count"]):
                            # Means the record does not need to updated and can be skipped
                            duplicates_skipped_count+=1
                        else:
                            # Means this was run with a partial day before and the record needs to be updated
                            update_vals = (username, record_date, users_date_summary_dict[username][record_date]["total_size"],
                                           users_date_summary_dict[username][record_date]["file_count"], username, record_date)
                            update_cursor.execute(file_stats_update_statement,update_vals)
                            duplicates_updated_count+=1
                else:
                    # The record did not exist - MOST LIKELY DUE TO
                    # missing username in userinfo if update user_stats have not been run recently
                    print("ERROR: " + str(err))
                    print("ERROR Input: " + str(input_vals))
                    print("ERROR Likely due to new user missing from user_info foreign key failure")
                    fail_inserts_count += 1

    db_connection.commit()
    print("duplicates_updated_count : " + str(duplicates_updated_count))
    print("duplicates_skipped_count : " + str(duplicates_skipped_count))
    print("fail_inserts_count : " + str(fail_inserts_count))
    print("Total insert_count : " + str(insert_count))
    return


def process_file_stats_data(start_date=datetime.datetime.combine(yesterday, datetime.datetime.min.time()),
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

    print("Start Date: " + str(start_date))
    print("End Date: " + str(end_date))

    uuid_user_mapping_dict = get_uuid_user_mappings(db_shock)
    users_date_summary_dict = get_nodes_data(db_shock,uuid_user_mapping_dict,start_date,end_date)
    #print("users date dict:" + str(users_date_summary_dict))

    upload_file_stats_data(users_date_summary_dict)


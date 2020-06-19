import os

import mysql.connector as mysql

sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]
metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]

'''
This script populates the user_id column in the user_info table.
It is built to backfill the records fro scratch
or to add on if the Unuique key gets disabled for some reason.
'''



def populate_user_ids():
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    counter_user_id = None
    first_populating = True
    first_record_not_done = True
    records_updated = 0
    records_evaluated = 0
    get_max_user_id_q = (
        "select max(user_id) from metrics.user_info "
    )
    cursor.execute(get_max_user_id_q)
    for row in cursor:
        counter_user_id = row[0]

        
    # If counter_user_id != 0 means not doing first populating. Need to add 1 to the number
    # If populating the first time you want to start the counter at zero, so unique key will be violated
    # If people try doing an insert into the table without setting the user_id properly.
    if counter_user_id != 0:
        first_populating = False
        first_record_not_done = False

    # Prepare the insert statement
    update_cursor = db_connection.cursor(prepared=True)
    user_info_update_statement = (
        "update metrics.user_info set user_id = ? where username = ?;"
    )

    existing_records_list = list()
    #Get the records by signup_date sorted. Get 3 fields. Username, user_id, signup_date
    get_existing_records_q = (
        "select username, user_id from user_info order by signup_date"
    )
    cursor.execute(get_existing_records_q)
    for (cur_username, cur_user_id) in cursor:
        existing_records_list.append([cur_username,cur_user_id])

    if existing_records_list[0][1] == 0:
        #the first record should be zero and we can skip it.
        existing_records_list.pop(0)
        records_evaluated += 1
    else:
        print("Something is wrong, first record does not have a zero for user_id")
        exit()
        
    for (cur_username, cur_user_id) in existing_records_list:
        records_evaluated += 1
#        if (first_record_not_done and first_populating):
#            # keep the first record with a user_id of zero
#            first_record_not_done = False
#            continue
        if cur_user_id == 0:
            # meatns need to assign a real user_id to it
            # Do an update and increment the counter
            counter_user_id += 1
            update_val = (counter_user_id, cur_username)
            update_cursor.execute(
                user_info_update_statement, update_val
            )
            records_updated += 1
        else:
            #already has a record. No need to update a user_id
            continue
        
    print("Number of Records Updated : " + str(records_updated))
    print("Number of Records Evaluated : " + str(records_evaluated))
    db_connection.commit()
    return 1

populate_user_ids()


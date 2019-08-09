def update_app_category_mappings():
    import category_to_app_dict
    import mysql.connector as mysql
    import os
    metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']

    #connect to mysql
    db_connection = mysql.connect(
        host = "10.58.0.98",
        user = "metrics",
        passwd = metrics_mysql_password,
        database = "metrics"
    )

    cursor = db_connection.cursor()
    query = "use metrics"
    cursor.execute(query)
    query = "select count(*) from app_name_category_map"
    cursor.execute(query)
    prev_results = list()
    for (prev_results) in cursor:
        print("Previous Number of App Category Mappings: " + str(prev_results[0]))

    del_cursor = db_connection.cursor(buffered=True,dictionary=True)
    query = "delete from app_name_category_map"
    cursor.execute(query)

    mapping_count = 0;
    cat_app_dict = category_to_app_dict.create_app_dictionary_1()
#    print("RESULT: CAT_TO_APP : "+ str(cat_app_dict))

    prep_cursor = db_connection.cursor(prepared=True)
    insert_statement =  "insert into app_name_category_map " \
                        "(app_name, app_category) "\
                        "values(%s, %s);"
    #HAVE TO CLEAN THE DATA, it is dirty with duplicates
    new_distinct_mappings = dict()
    for category_name in cat_app_dict:
        for app_name in cat_app_dict[category_name]:    
            new_distinct_mappings[app_name+":"+category_name] = [app_name,category_name]

    for key in new_distinct_mappings:
        input =  (new_distinct_mappings[key][0], new_distinct_mappings[key][1])       
        prep_cursor.execute(insert_statement,input)
        mapping_count+= 1
    
    db_connection.commit()
    print("Post input mapping_count : " + str(mapping_count))

import time
start_time = time.time()
update_app_category_mappings()
print("--- app_cat_mapping time :  %s seconds ---" % (time.time() - start_time))

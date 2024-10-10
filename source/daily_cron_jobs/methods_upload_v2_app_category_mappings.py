import os
import requests
import pandas as pd
import mysql.connector as mysql
import time
import datetime
from biokbase.catalog.Client import Catalog
from biokbase.narrative_method_store.client import NarrativeMethodStore

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]

specific_string = "empty"

# Configure App Data: Function

def create_function_dictionary():
    # Create App Dictionary: Main function
    requests.packages.urllib3.disable_warnings()
    catalog = Catalog(url=os.environ["CATALOG_URL"])
    nms = NarrativeMethodStore(url=os.environ["NARRATIVE_METHOD_STORE"])

    apps = nms.list_methods({"tag": "release"})
#    apps = nms.list_methods({"tag": "beta"})
#    apps = nms.list_methods({"tag": "dev"})
#    apps = nms.list_methods({})

    global specific_string

    print("APPS : "+ str(apps))
    print("============================")

    category_app_dict = dict()
    #key category,=> dict("active"=>[list of apps], "inactive"=>[list_of_apps], "unknown" => [list of apps])

    apps_with_both_list = list()
    apps_with_none_list = list()
    apps_with_no_cats_list = list()
    
    for temp_app_dict in apps:
#        if temp_app_dict["id"] == "kb_uploadmethods/batch_import_assembly_from_staging":
#PRESENT  
#        if temp_app_dict["id"] == "kb_uploadmethods/batch_import_assemblies_from_staging":
#NOT PRESENT
#            temp_specific_string = str(temp_app_dict)
#            specific_string = temp_specific_string  + "\n"
        
        if temp_app_dict["id"] == "view_expression_gene_table_heatmap":
            print("DETAIL : " + str(temp_app_dict))
        

        app_id = temp_app_dict["id"]
        app_cat_list = temp_app_dict["categories"]

        if app_id == "BBTools/RQCFilter":
            print("BBTools/RQCFilter app categories : " + str(app_cat_list))

        if app_id == "view_expression_heatmap":
            print("view_expression_heatmap : " + str(app_cat_list))

        active_type = None
        active_flag_has_both = 0
        active_inactive_count = 0
        if "active" in app_cat_list:
            active_inactive_count += 1
        if "inactive" in app_cat_list:
            active_inactive_count += 1
        if "active" in app_cat_list and "inactive" in app_cat_list:
            active_flag_has_both = 1
            print("UH OH!!!!!!!! : " + str(app_id) + " is both active and inactive")
            apps_with_both_list.append(app_id)
            active_type = "both"
#            exit(0)
#        else:
        elif "active" in app_cat_list:
            #CURRENTLY SET IF APP HAS BOTH IS SEEN AS ACTIVE
            active_type = "active"
        elif "inactive" in app_cat_list:
            active_type = "inactive"
        if active_type == None:
            print("UH OH!!!!!!!! : " + str(app_id) + " is not active or inactive")
            apps_with_none_list.append(app_id)
            active_type = "none"
#            exit(0)
        if (len(app_cat_list) - active_inactive_count) <= 0:
            apps_with_no_cats_list.append(app_id)
        for category_name in app_cat_list:
            if category_name == "active" or category_name == "inactive":
                continue
            if category_name not in category_app_dict:
                category_app_dict[category_name] = dict()
            if active_type not in category_app_dict[category_name]:
                category_app_dict[category_name][active_type] = list()
            category_app_dict[category_name][active_type].append(app_id)

    # Deal with apps that have empty category list 
    if len(apps_with_no_cats_list) > 0:
        category_app_dict["Empty Category"] = dict()
        category_app_dict["Empty Category"]["no_category"] = apps_with_no_cats_list
        
    print("FINAL category_app_dict : " + str(category_app_dict))
    total_count = 0
    category_count = 0
#    for temp_cat in  app_dict:
    for temp_cat in sorted(category_app_dict):
        for active_type in category_app_dict[temp_cat]:
            temp_count = len(category_app_dict[temp_cat][active_type])
            total_count += temp_count
        category_count += 1
    print("Total count : " + str(total_count))
    print("category count : " + str(category_count))
#    print("specific_string : " + str(specific_string))
    print("apps_with_none_list : " + str(apps_with_none_list))
    print("apps_with_none count : " + str(len(apps_with_none_list)))
    print("apps_with_both_list : " + str(apps_with_both_list))
    print("apps_with_both count : " + str(len(apps_with_both_list)))
    print("apps_with_no_cats_list : " + str(apps_with_no_cats_list))
    print("apps_with_no_cats_list count : " + str(len(apps_with_no_cats_list)))
    return category_app_dict


def update_app_category_mappings():
    # get app catagory mappings
    cat_app_dict  = create_function_dictionary()

#    print("EXITING")
#    exit()
    
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    # get existing mappings
    existing_records_list = list()
    existing_name_cat_dict = dict()
#    query = "select concat(app_name, '::', app_category, '::', is_active) from app_name_category_map_v2;"
    query = "select app_name, app_category, is_active from app_name_category_map_v2;"
    cursor.execute(query)
    for row in cursor:
        full_key = row[0] + "::" + row[1] + "::" + str(row[2])
        name_cat_key = row[0] + "::" + row[1]
        existing_records_list.append(full_key)
        existing_name_cat_dict[name_cat_key] = row[2]
    existing_count = len(existing_records_list)
        
    # update all existing records to be inactive
#    update_query = "update app_name_category_map_v2 set is_active = False"
#    cursor.execute(update_query)
#    db_connection.commit()
    
    # update active records if they exist or insert new row if did not exist
    # update statement
#    update_prep_cursor = db_connection.cursor(prepared=True)
#    update_statement = (
#        "update app_name_category_map_v2 "
#        "set is_active = %s "
#        "where app_name = %s and "
#        "app_category = %s "
#    )


    # insert statement
    insert_prep_cursor = db_connection.cursor(prepared=True)

    insert_statement = (
        "insert into app_name_category_map_v2 "
        "(app_name, app_category, is_active) "
        "values(%s, %s, %s);"
    )

    # update statement
    update_prep_cursor = db_connection.cursor(prepared=True)

    update_statement = (
        "update app_name_category_map_v2 "
        "set is_active = %s where app_name = %s and app_category = %s;"
    )
    
    # cleanup/delete statement
    cleanup_prep_cursor = db_connection.cursor(prepared=True)
    cleanup_statement = (
        "delete from app_name_category_map_v2 "
        "where app_name = %s and app_category = %s and is_active = %s;"
        )
    
    insert_count = 0
    update_count = 0
    activity_dict =  {'active': 1, 'inactive': 0, 'both': 2, "none":-1, "no_category":-2}
    for category_name in cat_app_dict:
        for active_type in cat_app_dict[category_name]:
            for app_name in cat_app_dict[category_name][active_type]:
                temp_key = app_name + "::" + category_name + "::" + str(activity_dict[active_type])
                temp_name_cat_key = app_name + "::" + category_name
                if temp_name_cat_key in existing_name_cat_dict:
                    if activity_dict[active_type] != existing_name_cat_dict[temp_name_cat_key]:
                        # record needs to be updated
                        input = (activity_dict[active_type], app_name, category_name,)
                        update_prep_cursor.execute(update_statement, input)
                        update_count += 1
                    if temp_key in existing_records_list:
                        existing_records_list.remove(temp_key)
                elif temp_key in existing_records_list:
                    existing_records_list.remove(temp_key)
                    #REMOVE FOM EXISTING TO FIND LEFT OVERS                                                                        
                else:
                    # do insert
#                    print("INPUT : " + str(input))
                    input = (app_name, category_name, activity_dict[active_type])
                    insert_prep_cursor.execute(insert_statement, input)
                    insert_count += 1

    #Clean up that no longer exist
    cleanup_count = 0
    for temp_key in existing_records_list:
        cleanup_count += 1
        temp_app_name, temp_cat_name, temp_is_active = temp_key.split('::')
        input = (temp_app_name, temp_cat_name, int(temp_is_active))
        cleanup_prep_cursor.execute(cleanup_statement, input)

    db_connection.commit()
    print("Existing_count : " + str(existing_count))
    print("Insert_count : " + str(insert_count))
    print("Update_count : " + str(update_count))
    print("Cleanup_count : " + str(cleanup_count))



print("############################################")
print("App Category Mapping Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
update_app_category_mappings()
print("--- app_cat_mapping time :  %s seconds ---" % (time.time() - start_time))

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
def data_configure(app_df):
    category_mess = list(app_df.categories)
#    filters = ["inactive", "viewers"]
#    filters = ["inactive"]
    filters = ["viewers"]
#    filters = []
    my_idx_list, categories, app_ids = [], [], []

    for idx, lst in enumerate(category_mess):
        if any([True for e in lst if e in filters]):
            my_idx_list.append(idx)
        else:
            lst = [x for x in lst if "active" != x]
            if lst:
                categories.append(lst)
            else:
                my_idx_list.append(idx)

    modDF = app_df.drop(my_idx_list)
    modDF.categories = categories
    return modDF


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

    for temp_app_dict in apps:
        if temp_app_dict["id"] == "kb_uploadmethods/batch_import_assembly_from_staging":
#PRESENT  
#        if temp_app_dict["id"] == "kb_uploadmethods/batch_import_assemblies_from_staging":
#NOT PRESENT
            temp_specific_string = str(temp_app_dict)
            specific_string = temp_specific_string  + "\n"
    
    apps_datastruc = pd.DataFrame.from_dict(apps)
    ModDfApps = data_configure(apps_datastruc)
    ModDfApps.drop(
        [
            "app_type",
            "authors",
            "git_commit_hash",
            "icon",
            "input_types",
            "module_name",
            "name",
            "namespace",
            "output_types",
            "subtitle",
            "tooltip",
            "ver",
        ],
        axis=1,
        inplace=True,
    )
    keys = list(
        set([item for sublist in list(ModDfApps.categories) for item in sublist])
    )
    print("KEYS : " + str(keys))
    print("============================")
    app_dict = {k: [] for k in keys}

    print("app_dict : " + str(app_dict))
    print("============================")

    for i in ModDfApps.index.values:
        app_category_lst = ModDfApps["categories"][i]
        for category in app_category_lst:
            if category in app_dict.keys():
                app_dict[category].append(ModDfApps["id"][i])
                app_dict[category] = list(set(app_dict[category]))
            else:
                raise KeyError("{} not a KBase app category".format(category))
    print("FINAL  app_dict : " + str(app_dict))
    total_count = 0
    category_count = 0
#    for temp_cat in  app_dict:
    for temp_cat in sorted(app_dict):
        temp_count = len(app_dict[temp_cat])
        print(temp_cat + " : " +  str(temp_count))
        total_count += temp_count
        category_count += 1
    print("Total count : " + str(total_count))
    print("category count : " + str(category_count))
    print("specific_string : " + str(specific_string))
    return app_dict


def update_app_category_mappings():
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    # get existing mappings
    existing_records_list = list()
    query = "select concat(app_name, '::', app_category) " "from app_name_category_map"
    cursor.execute(query)
    for row in cursor:
        existing_records_list.append(row[0])

    # update all existing records to be inactive
    update_query = "update app_name_category_map set is_active = False"
    cursor.execute(update_query)
    db_connection.commit()

    cat_app_dict = create_function_dictionary()

    print("EXITING")
    exit()
    
    # update active records if they exist or insert new row if did not exist
    # update statement
    update_prep_cursor = db_connection.cursor(prepared=True)
    update_statement = (
        "update app_name_category_map "
        "set is_active = True "
        "where app_name = %s and "
        "app_category = %s "
    )
    # insert statement
    insert_prep_cursor = db_connection.cursor(prepared=True)
    existing_count = len(existing_records_list)
    insert_statement = (
        "insert into app_name_category_map "
        "(app_name, app_category, is_active) "
        "values(%s, %s, True);"
    )
    insert_count = 0
    update_count = 0
    for category_name in cat_app_dict:
        for app_name in cat_app_dict[category_name]:
            input = (app_name, category_name)
            if app_name + "::" + category_name in existing_records_list:
                # do update
                update_prep_cursor.execute(update_statement, input)
                update_count += 1
            else:
                # do insert
                insert_prep_cursor.execute(insert_statement, input)
                insert_count += 1

    db_connection.commit()
    print("Existing_count : " + str(existing_count))
    print("Insert_count : " + str(insert_count))
    print("Update_count : " + str(update_count))



print("############################################")
print("App Category Mapping Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
update_app_category_mappings()
print("--- app_cat_mapping time :  %s seconds ---" % (time.time() - start_time))

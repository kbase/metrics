# Configure App Data: Function
def data_configure(app_df):
    category_mess = list(app_df.categories)
    filters = ["inactive", "viewers"]
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


def create_app_dictionary():
    #Create App Dictionary: Main function
    import requests
    requests.packages.urllib3.disable_warnings()
    from biokbase.catalog.Client import Catalog
    from biokbase.narrative_method_store.client import NarrativeMethodStore
    catalog = Catalog(url = "https://kbase.us/services/catalog")
    nms = NarrativeMethodStore(url = "https://kbase.us/services/narrative_method_store/rpc")
    import pandas as pd

    apps = nms.list_methods({"tag": "release"})
    apps_datastruc = pd.DataFrame.from_dict(apps)
    ModDfApps = data_configure(apps_datastruc)
    ModDfApps.drop(['app_type', 'authors', 'git_commit_hash', 'icon', 'input_types', 'module_name', 'name', 'namespace',
                    'output_types', 'subtitle', 'tooltip', 'ver'], axis=1, inplace=True)
    keys = list(set([item for sublist in list(ModDfApps.categories) for item in sublist]))
    app_dict = {k: [] for k in keys}

    for i in ModDfApps.index.values:

        app_category_lst = ModDfApps["categories"][i]
        for category in app_category_lst:
            if category in app_dict.keys():
                app_dict[category].append(ModDfApps["id"][i])
                app_dict[category] = list(set(app_dict[category]))
            else:
                raise KeyError("{} not a KBase app category".format(category))

    return app_dict



def update_app_category_mappings():
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
    cat_app_dict = create_app_dictionary()

    prep_cursor = db_connection.cursor(prepared=True)
    insert_statement =  "insert into app_name_category_map " \
                        "(app_name, app_category) "\
                        "values(%s, %s);"

    for category_name in cat_app_dict:
        for app_name in cat_app_dict[category_name]:    
            input =  (app_name, category_name)       
            prep_cursor.execute(insert_statement,input)
            mapping_count+= 1
    
    db_connection.commit()
    print("Post input mapping_count : " + str(mapping_count))

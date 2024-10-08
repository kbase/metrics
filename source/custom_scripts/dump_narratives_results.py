#!/usr/local/bin/python

import os
import mysql.connector as mysql

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
sql_host = os.environ["SQL_HOST"]
metrics = os.environ["QUERY_ON"]


def dump_narratives_results():
    #Dumps the results from current narratives
    #It is a simple SQL table dump of a given query so we can supply users with custom tables.
    #Note that the SQL query itself and column headers portion need to be changed if you want to change
    #the query/results. Otherwise it is good to go.
    #It can be called simply with the bin shell script.
    #Read the README at the top level for an example.
    #docker-compose run --rm metrics ../bin/custom_scripts/dump_query_results.sh > query_results.txt
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host,  # "mysql1", #"localhost",
        user="metrics",  # "root",
        passwd=metrics_mysql_password,
        database="metrics",  # "datacamp"
    )

    cursor = db_connection.cursor()
    query = "use " + metrics
    cursor.execute(query)

    # CHANGE QUERY HERE
    #    Query for Adam Narratives dump of information:
    query = ("select * from metrics.workspaces_current_plus_users ")
#    query = ("select wc.* from metrics.user_info ui inner join metrics_reporting.workspaces_current wc on ui.username = wc.username "
#             "where ui.kb_internal_user = 0 and wc.narrative_version > 0 and is_deleted = 0 and is_temporary = 0")
    #    Headers for Adam's narratives query (Note if more columns added, may need to update this
    print("ws_id\tusername\tmod_date\tinitial_save_date\trecord_date\ttop_lvl_object_count\ttotal_object_count\tvisible_app_cells_count\tcode_cells_count\t"
          "narrative_version\thidden_object_count\tdeleted_object_count\ttotal_size\ttop_lvl_size\tis_public\tis_temporary\tis_deleted\tnumber_of_shares\t"
          "num_nar_obj_ids\tstatic_narratives_count\tstatic_narratives_views\tunique_object_types_count\t"
          "orig_saver_count\tnon_orig_saver_count\torig_saver_size_GB\tnon_orig_saver_size_GB")
          )
          
#          "num_nar_obj_ids\tstatic_narratives_count\tstatic_narratives_views\tunique_object_types_count")

    cursor.execute(query)
    row_values = list()

    for row_values in cursor:
        temp_string = ""
        for i in range(len(row_values) - 1):
            if row_values[i] is not None:
                temp_string += str(row_values[i])
            temp_string += "\t"
        if row_values[-1] is not None:
            temp_string += str(row_values[-1])
        print(temp_string)
    return 1


dump_narratives_results()

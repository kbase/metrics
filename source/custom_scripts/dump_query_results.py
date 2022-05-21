#!/usr/local/bin/python

import os
import mysql.connector as mysql

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]
sql_host = os.environ["SQL_HOST"]
metrics = os.environ["QUERY_ON"]


def dump_query_results():
    """
    It is a simple SQL table dump of a given query so we can supply users with custom tables.
    Note that the SQL query itself and column headers portion need to be changed if you want to change
    the query/results. Otherwise it is good to go.
    It can be called simply with the bin shell script.
    Read the README at the top level for an example.
    docker-compose run --rm metrics ../bin/custom_scripts/dump_query_results.sh > query_results.txt

    """
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
    #    query = "select username, display_name, email, orcid, kb_internal_user, institution, country, signup_date, last_signin_date from user_info order by signup_date"
    #    Query for Adam Narratives dump of information:
    #    select wc.* from metrics.user_info ui inner join metrics_reporting.workspaces_current wc on ui.username = wc.username
    #    where ui.kb_internal_user = 0 and wc.narrative_version > 0 and is_deleted = 0 and is_temporary = 0;
    #query = ("select * from metrics_reporting.narrative_app_flows")
    query = ("select * from metrics_reporting.user_super_summary")
    # CHANGE COLUMN HEADERS HERE TO MATCH QUERY HEADERS
    #    print("username\temail\tlast_signin_date\tmax_last_seen\tHasBeenSeen")
    #    print("ws_id\tusername\tmod_date\tinitial_save_date\trecord_date\ttop_lvl_object_count\ttotal_object_count\tvisible_app_cells_count\tnarrative_version\thidden_object_count\tdeleted_object_count\ttotal_size\ttop_lvl_size\tis_public\tis_temporary\tnumber_of_shares")
    #    Headers for Adam's narratives query (Note if more columns added, may need to update this
    #    print(
    #        "ws_id\tusername\tmod_date\tinitial_save_date\trecord_date\ttop_lvl_object_count\ttotal_object_count\tvisible_app_cells_count\tcode_cells_count\t"
    #        "narrative_version\thidden_object_count\tdeleted_object_count\ttotal_size\ttop_lvl_size\tis_public\tis_temporary\tis_deleted\tnumber_of_shares\t"
    #        "num_nar_obj_ids\tstatic_narratives_count"
    #    )
    #    HEADERS FOR user_super_summary
    print(
        "username\tdisplay_name\temail\tkb_internal_user\tuser_id\tglobus_login\tgoogle_login\torcid\tsession_info_country\tcountry\tstate\t"
        "institution\tdepartment\tjob_title\thow_u_hear_selected\thow_u_hear_other\tdev_token_first_seen\tsignup_date\tlast_signin_date\tdays_signin_minus_signup\t"
        "days_since_last_signin\tnum_orgs\tnarrative_count\tshared_count\tnarratives_shared\tfirst_narrative_made_date\tlast_narrative_made_date\t"
        "last_narrative_modified_date\ttotal_narrative_objects_count\ttop_lvl_narrative_objects_count\ttotal_narrative_objects_size\t"
        "top_lvl_narrative_objects_size\ttotal_narrative_count\ttotal_public_narrative_count\tdistinct_static_narratives_count\t"
        "static_narratives_created_count\ttotal_visible_app_cells\ttotal_code_cells_count\tfirst_file_date\tlast_file_date\t"
        "total_file_sizes_MB\ttotal_file_count\tmost_used_app\tdistinct_apps_used\ttotal_apps_run_all_time\ttotal_apps_run_last365\t"
        "total_apps_run_last90\ttotal_apps_run_last30\ttotal_app_errors_all_time\tfirst_app_run\tlast_app_run\ttotal_run_time_hours\t"
        "total_queue_time_hours\ttotal_CPU_hours\tsession_count_all_time\tsession_count_last_year\tsession_count_last_90\tsession_count_last_30"
    )
    #Header for Adam's narrative_app_flow
    #print("ws_id\tusername\tapp_name\tfunc_name\tstart_date\tfinish_date") 

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


dump_query_results()

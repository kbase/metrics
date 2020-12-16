import os
import mysql.connector as mysql
import time
import datetime

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]

sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]


def make_reporting_tables():
    """
    Makes tables instead of views so long running view statements become fast against a smaller table
    """
    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    stats_app_function_git_combo_create_statement = (
        "create or replace table metrics_reporting.stats_app_function_git_combo as "
        "select art.func_name, art.app_name, art.git_commit_hash, "
        "art.app_count as success_app_count,art.avg_run_time, art.stdev_run_time, "
        "art.last_finish_date, fpa.fail_pct, fpa.tot_cnt as all_app_run_count,  "
        "nuu.all_users_count, non_kb_user_count, "
        "fpa.avg_queue_time, fpa.avg_reserved_cpu "
        "from metrics.hv_app_run_time_app_function_git_combo art inner join "
        "metrics.hv_fail_pct_app_function_git_combo fpa on "
        "art.app_name = fpa.app_name "
        "and art.func_name = fpa.func_name "
        "and art.git_commit_hash = fpa.git_commit_hash "
        "inner join metrics.hv_number_users_using_app_function_git_combo nuu on "
        "art.app_name = nuu.app_name "
        "and art.func_name = nuu.func_name "
        "and art.git_commit_hash = nuu.git_commit_hash "
        "left outer join metrics.hv_number_nonKB_users_using_app_function_git_combo nnu on "
        "art.app_name = nnu.app_name "
        "and art.func_name = nnu.func_name "
        "and art.git_commit_hash = nnu.git_commit_hash "
    )
    cursor.execute(stats_app_function_git_combo_create_statement)
    print("stats_app_function_git_combo created")
    ###########

    stats_function_git_combo_create_statement = (
        "create or replace table metrics_reporting.stats_function_git_combo as "
        "select art.func_name, art.git_commit_hash, "
        "art.app_count as success_app_count, "
        "art.avg_run_time, art.stdev_run_time, "
        "art.last_finish_date, "
        "fpa.fail_pct, fpa.tot_cnt as all_app_run_count, "
        "nuu.all_users_count, non_kb_user_count, "
        "fpa.avg_queue_time, fpa.avg_reserved_cpu "
        "from metrics.hv_app_run_time_function_git_combo art inner join "
        "metrics.hv_fail_pct_function_git_combo fpa on "
        "art.func_name = fpa.func_name "
        "and art.git_commit_hash = fpa.git_commit_hash "
        "inner join "
        "metrics.hv_number_users_using_function_git_combo nuu on "
        "art.func_name = nuu.func_name "
        "and art.git_commit_hash = nuu.git_commit_hash "
        "left outer join "
        "metrics.hv_number_nonKB_users_using_function_git_combo nnu on "
        "art.func_name = nnu.func_name "
        "and art.git_commit_hash = nnu.git_commit_hash "
    )
    cursor.execute(stats_function_git_combo_create_statement)
    print("stats_function_git_combo created")

    #############

    stats_function_create_statement = (
        "create or replace table metrics_reporting.stats_function as "
        "select art.func_name, "
        "art.app_count as success_app_count, "
        "art.avg_run_time, art.stdev_run_time, "
        "art.last_finish_date, "
        "fpa.fail_pct, fpa.tot_cnt as all_app_run_count, "
        "nuu.all_users_count, non_kb_user_count, "
        "fpa.avg_queue_time, fpa.avg_reserved_cpu "
        "from metrics.hv_app_run_time_function art inner join "
        "metrics_reporting.fail_pct_function fpa on "
        "art.func_name = fpa.func_name "
        "inner join "
        "metrics_reporting.number_users_using_function nuu on "
        "art.func_name = nuu.func_name "
        "left outer join "
        "metrics.hv_number_nonKB_users_using_function nnu on "
        "art.func_name = nnu.func_name "
    )
    cursor.execute(stats_function_create_statement)
    print("stats_function created")
    ##################

    app_category_run_counts_create_statement = (
        "create or replace table metrics_reporting.app_category_run_counts as "
        "select aac.app_category, aac.total_app_run_cnt, nkbc.non_kb_internal_app_run_cnt "
        "from metrics.hv_all_app_category_run_counts aac "
        "left outer join "
        "metrics.hv_non_kb_internal_app_category_run_counts nkbc "
        "on aac.app_category = nkbc.app_category"
    )
    cursor.execute(app_category_run_counts_create_statement)
    print("app_category_run_counts created")
    ##################

    institution_app_cat_run_counts_create_statement = (
        "create or replace table metrics_reporting.institution_app_cat_run_counts as "
        "select ui.institution, "
        "IFNULL(acm.app_category,'unable to determine') as app_category, "
        "DATE_FORMAT(`finish_date`,'%Y-%m') as app_run_month, "
        "count(*) as app_category_run_cnt "
        "from metrics.user_info ui "
        "inner join "
        "metrics.user_app_usage uau "
        "on ui.username = uau.username "
        "left outer join "
        "metrics.app_name_category_map acm "
        "on IFNULL(uau.app_name,'not specified') = acm.app_name "
        "where ui.exclude = False "
        "group by ui.institution, app_category, app_run_month "
    )
    cursor.execute(institution_app_cat_run_counts_create_statement)
    print("institution_app_cat_run_counts created")

    #################
    hv_app_category_unique_users_weekly_create_statement = (
        "create or replace table metrics.hv_app_category_unique_users_weekly as "
        "select distinct DATE_FORMAT(`finish_date`,'%Y-%u') as week_run, "
        "IFNULL(app_category,'None') as app_category, uau.username "
        "from metrics.user_app_usage uau inner join "
        "metrics.user_info ui on uau.username = ui.username "
        "left outer join "
        "metrics.app_name_category_map anc on uau.app_name = anc.app_name "
        "where ui.kb_internal_user = 0 "
        "and func_name != 'kb_gtdbtk/run_kb_gtdbtk' "
    )
    cursor.execute(hv_app_category_unique_users_weekly_create_statement)
    print("hv_app_category_unique_users_weekly created")

    app_category_unique_users_weekly_create_statement = (
        "create or replace table metrics_reporting.app_category_unique_users_weekly as "
        "select week_run, app_category, count(*) as unique_users "
        "from metrics.hv_app_category_unique_users_weekly "
        "group by week_run, app_category "
    )
    cursor.execute(app_category_unique_users_weekly_create_statement)
    print("app_category_unique_users_weekly created")

    #################
    app_category_run_hours_weekly_create_statement = (
        "create or replace table metrics_reporting.app_category_run_hours_weekly as "
        "select distinct DATE_FORMAT(`finish_date`,'%Y-%u') as week_run, "
        "IFNULL(app_category,'None') as app_category, round(sum(run_time)/3600,1) as run_hours "
        "from metrics.user_app_usage uau inner join "
        "metrics.user_info ui on uau.username = ui.username "
        "left outer join "
        "metrics.app_name_category_map anc on uau.app_name = anc.app_name "
        "where ui.kb_internal_user = 0 "
        "and func_name != 'kb_gtdbtk/run_kb_gtdbtk' "
        "group by app_category, week_run "
    )
    cursor.execute(app_category_run_hours_weekly_create_statement)
    print("app_category_run_hours_weekly created")


    ################
    narrative_app_flows_create_statement = (
        "create or replace table metrics_reporting.narrative_app_flows as "
        "select uau.ws_id, uau.username, uau.app_name, uau.func_name, uau.start_date, uau.finish_date "
        "from metrics.user_info ui "
        "inner join metrics.user_app_usage uau "
        "on ui.username = uau.username "
        "inner join metrics_reporting.workspaces_current wc "
        "on wc.ws_id = uau.ws_id "
        "where ui.kb_internal_user = 0 "
        "and uau.is_error = 0 "
        "and wc.narrative_version > 0 "
        "order by ws_id, start_date"
    )
    cursor.execute(narrative_app_flows_create_statement)
    print("narrative_app_flows created")
    
    return


import time
import datetime

print("############################################")
print("Making Report Tables (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
make_reporting_tables()
print(
    "--- making reporting tables count time :  %s seconds ---"
    % (time.time() - start_time)
)

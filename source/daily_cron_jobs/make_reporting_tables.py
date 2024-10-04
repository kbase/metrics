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

    ##################
    # a whole bunch of tables related user_super_summary (some helpers that can also be used stadn alone)
    ##################

    # App stats for user_super_summary  
    hv_user_app_summaries_create_statement = (
        "create or replace table metrics.hv_user_app_summaries as "
        "select username, "
        "min(finish_date) as first_app_run, "
        "max(finish_date) as last_app_run, "
        "count(*) as total_app_runs, "
        "sum(is_error) as total_error_runs, "
        "sum(run_time)/(3600) as total_run_time_hours, "
        "sum(queue_time)/(3600) as total_queue_time_hours, "
        "sum(reserved_cpu * run_time)/(3600) as total_CPU_hours "
        "from metrics.user_app_usage "
        "group by username"
    )
    cursor.execute(hv_user_app_summaries_create_statement)
    print("hv_user_app_summaries created")

    hv_user_app_counts_create_statement = (
        "create or replace table metrics.hv_user_app_counts as " 
        "select func_name, username, "
        "count(*) as user_app_count, "
        "sum(is_error) as user_error_count, "
        "min(finish_date) as first_app_run, "
        "max(finish_date) as last_app_run "
        "from metrics.user_app_usage "
        "group by  func_name, username")
    cursor.execute(hv_user_app_counts_create_statement)
    print("hv_user_app_counts created")

    hv_users_alltime_app_counts_create_statement = (
        "create or replace table metrics.hv_users_alltime_app_counts as "
        "select username, count(*) as app_count_all_time "
        "from metrics.user_app_usage uau_all "
        "group by username")
    cursor.execute(hv_users_alltime_app_counts_create_statement)
    print("hv_users_alltime_app_counts created")

    hv_users_last365days_app_counts_create_statement = (
        "create or replace table metrics.hv_users_last365days_app_counts as "
        "select username, count(*) as app_count_last_365 "
        "from metrics.user_app_usage uau_365 "
        "where finish_date >= (NOW() - INTERVAL 365 DAY) "
        "group by username ")
    cursor.execute(hv_users_last365days_app_counts_create_statement)
    print("hv_users_last365days_app_counts created")

    hv_users_last_90days_app_counts_create_statement = (
        "create or replace table metrics.hv_users_last_90days_app_counts as " 
        "select username, count(*) as app_count_last_90 "
        "from metrics.user_app_usage uau_90 "
        "where finish_date >= (NOW() - INTERVAL 90 DAY) "
        "group by username ")
    cursor.execute(hv_users_last_90days_app_counts_create_statement)
    print("hv_users_last_90days_app_counts created")

    hv_users_last_30days_app_counts_create_statement = (
        "create or replace table metrics.hv_users_last_30days_app_counts as "
        "select username, count(*) as app_count_last_30 "
        "from metrics.user_app_usage uau_30 "
        "where finish_date >= (NOW() - INTERVAL 30 DAY) "
        "group by username ")
    cursor.execute(hv_users_last_30days_app_counts_create_statement)
    print("hv_users_last_30days_app_counts created")

    users_narratives_summary_create_statement = (
        "create or replace table metrics_reporting.users_narratives_summary as "
        "select wc.username, "
        "ui.kb_internal_user, "
        "min(initial_save_date) as first_narrative_made_date, "
        "max(initial_save_date) as last_narrative_made_date, "
        "max(mod_date) as last_narrative_modified_date, "
        "sum(total_object_count) as total_narrative_objects_count, "
        "sum(top_lvl_object_count) as top_lvl_narrative_objects_count, "
        "sum(total_size) as total_narrative_objects_size, "        
        "sum(top_lvl_size) as top_lvl_narrative_objects_size, "
        "count(*) as total_narrative_count, "
        "sum(is_public) as total_public_narrative_count, "
        "sum(ceiling(static_narratives_count/(static_narratives_count + .00000000000000000000001))) as distinct_static_narratives_count, "
        "sum(static_narratives_count) as static_narratives_created_count, "
        "sum(visible_app_cells_count) as total_visible_app_cells, "
        "sum(code_cells_count) as total_code_cells_count "
        "from metrics_reporting.workspaces_current wc "
        "inner join metrics.user_info ui "
        "on wc.username = ui.username "
        "where narrative_version > 0 "
        "and is_deleted = 0 "
        "and is_temporary = 0 "
        "group by wc.username, ui.kb_internal_user ")
    cursor.execute(users_narratives_summary_create_statement)
    print("users_narratives_summary_create_statement created")
    
    user_super_summary_create_statement = (
        "create or replace table metrics_reporting.user_super_summary as "
        "select uip.username, uip.display_name, "
        "uip.email, uip.kb_internal_user, uip.user_id, "
        "uip.globus_login, uip.google_login, uip.orcid, "
        "uip.session_info_country, uip.country, uip.state, "
        "uip.institution, uip.department, uip.job_title, "
        "uip.how_u_hear_selected, uip.how_u_hear_other, "
        "uip.dev_token_first_seen, "
        "uip.signup_date, uip.last_signin_date, "
        "uip.days_signin_minus_signup, days_since_last_signin, "
        "usssc.num_orgs, usssc.narrative_count, "
        "usssc.shared_count, usssc.narratives_shared, "
        "uns.first_narrative_made_date, uns.last_narrative_made_date, "
        "uns.last_narrative_modified_date, "
        "uns.total_narrative_objects_count,uns.top_lvl_narrative_objects_count, "
        "uns.total_narrative_objects_size, uns.top_lvl_narrative_objects_size, "
        "uns.total_narrative_count, uns.total_public_narrative_count, "
        "uns.distinct_static_narratives_count, uns.static_narratives_created_count, "
        "uns.total_visible_app_cells, uns.total_code_cells_count, "
        "bus.first_file_date, bus.last_file_date, "
        "bus.total_file_sizes_MB, bus.total_file_count, "
        "bdu.orig_saver_count as blobstore_orig_saver_count, "
        "bdu.non_orig_saver_count as blobstore_non_orig_saver_count, "
        "bdu.orig_saver_size_GB as blobstore_orig_saver_size_GB, "
        "bdu.non_orig_saver_size_GB as blobstore_non_orig_saver_size_GB, "
        "umua.mu_func_name as most_used_app, "
        "udauc.distinct_apps_used, "
        "uapc.total_apps_run_all_time, uapc.total_apps_run_last365, "
        "uapc.total_apps_run_last90, uapc.total_apps_run_last30, "
        "uas.total_error_runs as total_app_errors_all_time, "
        "uas.first_app_run, uas.last_app_run, "
        "uas.total_run_time_hours, uas.total_queue_time_hours, "
        "uas.total_CPU_hours, "
        "uscat.session_count_all_time, "
        "uscly.session_count_last_year, "
        "usc90.session_count_last_90, "
        "usc30.session_count_last_30 "
        "from metrics_reporting.user_info_plus uip "
        "inner join metrics.user_system_summary_stats_current usssc "
        "on uip.username = usssc.username "
        "left outer join metrics_reporting.users_narratives_summary uns "
        "on uip.username = uns.username "
        "left outer join metrics.hv_blobstore_user_summaries bus "
        "on uip.username = bus.username "
        "left outer join metrics_reporting.users_app_counts_periods uapc "
        "on uip.username = uapc.username "
        "left outer join metrics.hv_user_app_summaries uas "
        "on uip.username = uas.username "
        "left outer join metrics.hv_user_most_used_app umua "
        "on uip.username = umua.username "
        "left outer join metrics.hv_users_distinct_apps_used_count udauc "
        "on uip.username = udauc.username "
        "left outer join metrics.hv_user_session_count_all_time uscat "
        "on uip.username = uscat.username "
        "left outer join metrics.hv_user_session_count_last_year uscly "
        "on uip.username = uscly.username "
        "left outer join metrics.hv_user_session_count_last_90 usc90 "
        "on uip.username = usc90.username "
        "left outer join metrics.hv_user_session_count_last_30 usc30 "
        "on uip.username = usc30.username "
        "left outer join metrics.blobstore_detail_by_user bdu "
        "on uip.username = bdu.saver_username "
        "where uip.exclude != 1 ")    
    cursor.execute(user_super_summary_create_statement)
    print("user_super_summary_create_statement created")


    # Blobstroe detial related tables
    blobstore_detail_by_ws_create_statement = (
        "create or replace table blobstore_detail_by_ws as "
        "(select in_q.ws_id, sum(in_q.orig_saver_count) as orig_saver_count, "
        "sum(in_q.non_orig_saver_count) as non_orig_saver_count, "
        "sum(in_q.orig_saver_size_GB) as orig_saver_size_GB, "
        "sum(in_q.non_orig_saver_size_GB) as non_orig_saver_size_GB, "
        "sum(in_q.total_blobstore_size_GB) as total_blobstore_size_GB "
        "from ("
        "select ws_id, DATE_FORMAT(`save_date`,'%Y-%m') as month, "
        "sum(orig_saver) as orig_saver_count, 0 - sum((orig_saver - 1)) as non_orig_saver_count, "
        "sum(orig_saver * size)/1000000000 as orig_saver_size_GB, "
        "0 - sum((orig_saver - 1) * size)/1000000000 as non_orig_saver_size_GB, "
        "sum(size)/1000000000 as total_blobstore_size_GB "
        "from blobstore_detail bd "
        "group by ws_id, month) in_q "
        "group by ws_id ) ")
    cursor.execute(blobstore_detail_by_ws_create_statement)
    print("blobstore_detail_by_ws_create_statement created")

    blobstore_detail_by_user_monthly_create_statement = (
        "create or replace table blobstore_detail_by_user_monthly as "
        "(select saver_username, DATE_FORMAT(`save_date`,'%Y-%m') as month, "
        "sum(orig_saver) as orig_saver_count, 0 - sum((orig_saver - 1)) as non_orig_saver_count, "
        "sum(orig_saver * size)/1000000000 as orig_saver_size_GB, "
        "0 - sum((orig_saver - 1) * size)/1000000000 as non_orig_saver_size_GB, "
        "sum(size)/1000000000 as total_blobstore_size_GB "
        "from blobstore_detail bd "
        "group by saver_username, month) ")
    cursor.execute(blobstore_detail_by_user_monthly_create_statement)
    print("blobstore_detail_by_user_monthly_create_statement created")
    
    blobstore_detail_by_user_create_statement = (
        "create or replace table blobstore_detail_by_user as "
        "(select saver_username, "
        "sum(orig_saver_count) as orig_saver_count, sum(non_orig_saver_count) as non_orig_saver_count, "
        "sum(orig_saver_size_GB) as orig_saver_size_GB, "
        "sum(non_orig_saver_size_GB) as non_orig_saver_size_GB, "
        "sum(total_blobstore_size_GB) as total_blobstore_size_GB "
        "from blobstore_detail_by_user_monthly "
        "group by saver_username) ")
    cursor.execute(blobstore_detail_by_user_create_statement)
    print("blobstore_detail_by_user_create_statement created")

    blobstore_detail_by_object_type_monthly_create_statement = (
        "create or replace table blobstore_detail_by_object_type_monthly as "
        "(select LEFT(object_type,LOCATE('-',object_type) - 1) as object_type, "
        "DATE_FORMAT(`save_date`,'%Y-%m') as month, "
        "sum(orig_saver) as orig_saver_count, 0 - sum((orig_saver - 1)) as non_orig_saver_count, "
        "sum(orig_saver * size)/1000000000 as orig_saver_size_GB, "
        "0 - sum((orig_saver - 1) * size)/1000000000 as non_orig_saver_size_GB, "
        "sum(size)/1000000000 as total_blobstore_size_GB "
        "from blobstore_detail bd "
        "group by object_type, month) ")
    cursor.execute(blobstore_detail_by_object_type_monthly_create_statement)
    print("blobstore_detail_by_object_type_monthly_create_statement created")




    
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

#SYSTEM_SUMMARY_STATS (current summary stats the most important)

#IN METRICS
CREATE OR REPLACE VIEW metrics.hv_user_system_max_date as
select max(u_cdate.record_date) as maxdate, u_cdate.username
from metrics.user_system_summary_stats u_cdate
group by u_cdate.username;

#IN METRICS
CREATE OR REPLACE VIEW metrics.user_system_summary_stats_current as
select usss.*
from metrics.user_system_summary_stats usss,
metrics.hv_user_system_max_date usmd
where usss.username = usmd.username
and usss.record_date = usmd.maxdate;

#IN METRICS
CREATE OR REPLACE VIEW  metrics.hv_user_system_min_date as
select min(u_cdate.record_date) as mindate, u_cdate.username
from metrics.user_system_summary_stats u_cdate
group by u_cdate.username;

#IN METRICS
CREATE OR REPLACE VIEW  metrics.hv_user_system_summary_stats_first as
select usss.*
from metrics.user_system_summary_stats usss,
metrics.hv_user_system_min_date usmd
where usss.username = usmd.username
and usss.record_date = usmd.mindate;


------------------------------------
# VIEWS OF REGULAR TABLES 

#IN METRICS
create or replace view metrics.hv_user_app_error_count as
select count(*) as total_app_err_count, username 
from metrics.user_app_usage 
where is_error = True
group by username;

#IN METRICS
create or replace view metrics.hv_user_app_count as
select count(*) as total_app_count, username 
from metrics.user_app_usage 
group by username;

#IN METRICS_REPORTING
create or replace view metrics_reporting.user_info_plus as
select ui.* ,
round((UNIX_TIMESTAMP(ui.last_signin_date) - UNIX_TIMESTAMP(ui.signup_date))/86400,2) as days_signin_minus_signup,
ceil((UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(last_signin_date))/86400) as days_since_last_signin,
IFNULL(uac.total_app_count,0) as total_app_count,
IFNULL(uec.total_app_err_count,0) as total_app_err_count
from metrics.user_info ui 
left outer join 
metrics.hv_user_app_count uac on ui.username = uac.username
left outer join   
metrics.hv_user_app_error_count uec on ui.username = uec.username
where exclude = False
order by signup_date;


#IN METRICS_REPORTING
create or replace view metrics_reporting.user_info_summary_stats as
select ui.username, ui.display_name, ui.email, ui.orcid,
ui.user_id, ui.kb_internal_user, ui.institution, ui.country,
ui.signup_date, ui.last_signin_date, 
round((UNIX_TIMESTAMP(ui.last_signin_date) - UNIX_TIMESTAMP(ui.signup_date))/86400,2) as days_signin_minus_signup,
ceil((NOW() - last_signin_date)/86400) as days_since_last_signin,
uc.num_orgs, uc.narrative_count, uc.shared_count,
uc.narratives_shared, uc.record_date
from metrics.user_info ui inner join 
metrics.user_system_summary_stats_current uc
on ui.username = uc.username
where exclude = False;

------------------------------
# USER SIGNUPS AND RETENTIONS.

#IN METRICS
create or replace view metrics.hv_user_monthly_signups as
select 
DATE_FORMAT(`signup_date`,'%Y-%m') as signup_month,
count(*) as total_signups 
from metrics.user_info 
where kb_internal_user = False 
and exclude = False
group by signup_month;

#IN METRICS
create or replace view metrics.hv_user_monthly_signups_still_active as
select 
DATE_FORMAT(`signup_date`,'%Y-%m') as signup_month,
count(*) as active_in_last_90_days_count 
from metrics_reporting.user_info_plus 
where kb_internal_user = False 
and days_since_last_signin < 90
and exclude = False
group by signup_month;

#IN METRICS
create or replace view metrics.hv_user_monthly_signups_that_returned as
select 
DATE_FORMAT(`signup_date`,'%Y-%m') as signup_month,
count(*) as users_returned_since_signin_count 
from metrics_reporting.user_info_plus 
where kb_internal_user = False 
and days_signin_minus_signup >= 1
and exclude = False
group by signup_month;

#IN METRICS_REPORTING
create or replace view metrics_reporting.user_monthly_signups_retention as
select us.signup_month, us.total_signups, 
ua.active_in_last_90_days_count, 
(ua.active_in_last_90_days_count/us.total_signups) * 100 as pct_active_last_90_days,
ur.users_returned_since_signin_count,
(ur.users_returned_since_signin_count/us.total_signups) * 100 as pct_returned_since_signin
from metrics.hv_user_monthly_signups us left outer join 
metrics.hv_user_monthly_signups_still_active ua 
on us.signup_month = ua.signup_month left outer join
metrics.hv_user_monthly_signups_that_returned ur
on us.signup_month = ur.signup_month;

------------------------------
#APP STATS APPNAME FUNCTION GIT COMBO LEVEL

#IN METRICS
create or replace view metrics.hv_distinct_users_using_app_func_git as
select distinct username, app_name, func_name, git_commit_hash
from metrics.user_app_usage;

#IN METRICS
create or replace view metrics.hv_number_users_using_app_function_git_combo as
select count(*) as all_users_count, IFNULL(app_name,"None") as app_name, func_name, git_commit_hash 
from metrics.hv_distinct_users_using_app_func_git
group by app_name, func_name, git_commit_hash;

#IN METRICS
create or replace view metrics.hv_distinct_nonKB_users_using_app_func_git as
select distinct ui.username, app_name, func_name, git_commit_hash
from metrics.user_app_usage ua 
inner join metrics.user_info ui on
ua.username = ui.username 
where ui.kb_internal_user = False
and ui.exclude = False;

#IN METRICS
create or replace view metrics.hv_number_nonKB_users_using_app_function_git_combo as
select count(*)as non_kb_user_count, IFNULL(app_name,"None") as app_name, func_name, git_commit_hash 
from metrics.hv_distinct_nonKB_users_using_app_func_git
group by app_name, func_name, git_commit_hash;

#IN METRICS
create or replace view metrics.hv_total_exec_app_function_git as
select count(*) as tot_cnt, IFNULL(app_name,"None") as app_name,
func_name, git_commit_hash, 
round(avg(queue_time),1) as avg_queue_time, 
round(avg(reserved_cpu),1) as avg_reserved_cpu
from metrics.user_app_usage
group by app_name, func_name, git_commit_hash;


#IN METRICS
create or replace view metrics.hv_fail_exec_app_function_git as
select count(*) as err_cnt, IFNULL(app_name,"None") as app_name, 
func_name, git_commit_hash 
from metrics.user_app_usage
where is_error = True
group by app_name, func_name, git_commit_hash;

#IN METRICS
create or replace view metrics.hv_fail_pct_app_function_git_combo as
select (IFNULL(fe.err_cnt,0)/te.tot_cnt) * 100 as fail_pct, 
te.tot_cnt, te.app_name, te.func_name, te.git_commit_hash,
te.avg_queue_time, te.avg_reserved_cpu
from metrics.hv_total_exec_app_function_git as te left outer join
metrics.hv_fail_exec_app_function_git as fe
on te.app_name = fe.app_name
and te.func_name = fe.func_name
and te.git_commit_hash = fe.git_commit_hash;

#IN METRICS
create or replace view metrics.hv_app_run_time_app_function_git_combo as
select AVG(run_time) as avg_run_time, STD(run_time) as stdev_run_time, 
max(finish_date) as last_finish_date,
count(*) as app_count, IFNULL(app_name,"None") as app_name, func_name, git_commit_hash 
from metrics.user_app_usage 
where is_error = False group by app_name, func_name, git_commit_hash;


#IN METRICS_REPORTING - THIS IS A TABLE THAT GETS MADE ONCE A DAY BY CRON JOB: source/make_reporting_tables.py
create or replace table metrics_reporting.stats_app_function_git_combo as
select art.func_name, art.app_name, art.git_commit_hash, 
art.app_count as success_app_count,
art.avg_run_time, art.stdev_run_time, 
art.last_finish_date,
fpa.fail_pct, fpa.tot_cnt as all_app_run_count, 
nuu.all_users_count, non_kb_user_count,
fpa.avg_queue_time, fpa.avg_reserved_cpu
from metrics.hv_app_run_time_app_function_git_combo art inner join
metrics.hv_fail_pct_app_function_git_combo fpa on 
art.app_name = fpa.app_name
and art.func_name = fpa.func_name
and art.git_commit_hash = fpa.git_commit_hash 
inner join
metrics.hv_number_users_using_app_function_git_combo nuu on
art.app_name = nuu.app_name
and art.func_name = nuu.func_name
and art.git_commit_hash = nuu.git_commit_hash 
left outer join
metrics.hv_number_nonKB_users_using_app_function_git_combo nnu on
art.app_name = nnu.app_name
and art.func_name = nnu.func_name
and art.git_commit_hash = nnu.git_commit_hash;


-------------------------------------------------------------
#APP STATS FUNCTION GIT COMBO LEVEL

#IN METRICS
create or replace view metrics.hv_distinct_users_using_func_git as
select distinct username, func_name, git_commit_hash
from metrics.user_app_usage;

#IN METRICS
create or replace view metrics.hv_number_users_using_function_git_combo as
select count(*) as all_users_count, func_name, git_commit_hash 
from metrics.hv_distinct_users_using_func_git
group by func_name, git_commit_hash;

#IN METRICS
create or replace view metrics.hv_distinct_nonKB_users_using_func_git as
select distinct ui.username, func_name, git_commit_hash
from metrics.user_app_usage ua 
inner join metrics.user_info ui on
ua.username = ui.username 
where ui.kb_internal_user = False
and ui.exclude = False;

#IN METRICS
create or replace view metrics.hv_number_nonKB_users_using_function_git_combo as
select count(*) as non_kb_user_count, func_name, git_commit_hash 
from metrics.hv_distinct_nonKB_users_using_func_git
group by func_name, git_commit_hash;

#IN METRICS
create or replace view metrics.hv_total_exec_function_git as
select count(*) as tot_cnt,  
func_name, git_commit_hash, 
round(avg(queue_time),1) as avg_queue_time, 
round(avg(reserved_cpu),1) as avg_reserved_cpu
from metrics.user_app_usage
group by func_name, git_commit_hash;

#IN METRICS
create or replace view metrics.hv_fail_exec_function_git as
select count(*) as err_cnt,  
func_name, git_commit_hash from metrics.user_app_usage
where is_error = True
group by func_name, git_commit_hash;

#IN METRICS
create or replace view metrics.hv_fail_pct_function_git_combo as
select (IFNULL(fe.err_cnt,0)/te.tot_cnt) * 100 as fail_pct, 
te.tot_cnt, te.func_name, te.git_commit_hash,
te.avg_queue_time, te.avg_reserved_cpu
from metrics.hv_total_exec_function_git as te left outer join
metrics.hv_fail_exec_function_git as fe
on te.func_name = fe.func_name
and te.git_commit_hash = fe.git_commit_hash;

#IN METRICS
create or replace view metrics.hv_app_run_time_function_git_combo as
select AVG(run_time) as avg_run_time, STD(run_time) as stdev_run_time, 
count(*) as app_count, max(finish_date) as last_finish_date,
func_name, git_commit_hash 
from metrics.user_app_usage 
where is_error = False group by func_name, git_commit_hash;

#IN METRICS_REPORTING - THIS IS A TABLE THAT GETS MADE ONCE A DAY BY CRON JOB: source/make_reporting_tables.py
create or replace table metrics_reporting.stats_function_git_combo as
select art.func_name, art.git_commit_hash, 
art.app_count as success_app_count,
art.avg_run_time, art.stdev_run_time, 
art.last_finish_date,
fpa.fail_pct, fpa.tot_cnt as all_app_run_count, 
nuu.all_users_count, non_kb_user_count,
fpa.avg_queue_time, fpa.avg_reserved_cpu
from metrics.hv_app_run_time_function_git_combo art inner join
metrics.hv_fail_pct_function_git_combo fpa on 
art.func_name = fpa.func_name
and art.git_commit_hash = fpa.git_commit_hash 
inner join
metrics.hv_number_users_using_function_git_combo nuu on 
art.func_name = nuu.func_name
and art.git_commit_hash = nuu.git_commit_hash 
left outer join
metrics.hv_number_nonKB_users_using_function_git_combo nnu on
art.func_name = nnu.func_name
and art.git_commit_hash = nnu.git_commit_hash;



-----------------------------------
#APP STATS FUNCTION LEVEL ONLY 

#IN METRICS
create or replace view metrics.hv_distinct_users_using_func as
select distinct username, func_name
from metrics.user_app_usage;

#IN METRICS REPORTING
create or replace view metrics_reporting.number_users_using_function as
select count(*) as all_users_count, func_name 
from metrics.hv_distinct_users_using_func
group by func_name;

#IN METRICS
create or replace view metrics.hv_distinct_nonKB_users_using_func as
select distinct ui.username, func_name
from metrics.user_app_usage ua 
inner join metrics.user_info ui on
ua.username = ui.username 
where ui.kb_internal_user = False
ßand ui.exclude = False;

#IN METRICS
create or replace view metrics.hv_number_nonKB_users_using_function as
select count(*)as non_kb_user_count, func_name
from metrics.hv_distinct_nonKB_users_using_func
group by func_name;

#IN METRICS
create or replace view metrics.hv_total_exec_function as
select count(*) as tot_cnt, func_name,
round(avg(queue_time),1) as avg_queue_time,
round(avg(reserved_cpu),1) as avg_reserved_cpu
from metrics.user_app_usage
group by func_name;

#IN METRICS
create or replace view metrics.hv_fail_exec_function as
select count(*) as err_cnt, func_name 
from metrics.user_app_usage
where is_error = True
group by func_name;

#IN METRICS REPORTING
create or replace view metrics_reporting.fail_pct_function as
select (IFNULL(fe.err_cnt,0)/te.tot_cnt) * 100 as fail_pct, 
te.tot_cnt, te.func_name,
te.avg_queue_time, te.avg_reserved_cpu
from metrics.hv_total_exec_function as te left outer join
metrics.hv_fail_exec_function as fe
on te.func_name = fe.func_name;

#METRICS
create or replace view metrics.hv_app_run_time_function as
select AVG(run_time) as avg_run_time, STD(run_time) as stdev_run_time, 
count(*) as app_count, max(finish_date) as last_finish_date,
func_name 
from metrics.user_app_usage 
where is_error = False group by func_name;

#IN METRICS_REPORTING - THIS IS A TABLE THAT GETS MADE ONCE A DAY BY CRON JOB: source/make_reporting_tables.py
create or replace table metrics_reporting.stats_function as
select art.func_name, 
art.app_count as success_app_count,
art.avg_run_time, art.stdev_run_time, 
art.last_finish_date,
fpa.fail_pct, fpa.tot_cnt as all_app_run_count, 
nuu.all_users_count, non_kb_user_count,
fpa.avg_queue_time, fpa.avg_reserved_cpu
from metrics.hv_app_run_time_function art inner join
metrics_reporting.fail_pct_function fpa on 
art.func_name = fpa.func_name
inner join
metrics_reporting.number_users_using_function nuu on 
art.func_name = nuu.func_name
left outer join
metrics.hv_number_nonKB_users_using_function nnu on
art.func_name = nnu.func_name;

------------------------------------
#IN METRICS
create or replace view metrics.hv_app_usage_by_month as
select DATE_FORMAT(`finish_date`,'%Y-%m') as finish_month,
count(*) as number_app_runs,
round(avg(queue_time),1) as avg_queue_time
from metrics.user_app_usage
group by finish_month;

#IN METRICS
create or replace view metrics.hv_non_kbase_users_app_usage_by_month as
select DATE_FORMAT(`finish_date`,'%Y-%m') as finish_month,
count(*) as number_app_runs
from metrics.user_app_usage ua inner join metrics.user_info ui
on ua.username = ui.username
where ui.kb_internal_user = False
and ui.exclude = False
group by finish_month;

#IN METRICS
create or replace view metrics.hv_app_usage_errors_by_month as
select DATE_FORMAT(`finish_date`,'%Y-%m') as finish_month,
count(*) as number_app_runs
from metrics.user_app_usage
where is_error = True
group by finish_month;

#IN METRICS
create or replace view metrics.hv_non_kbase_users_app_usage_errors_by_month as
select DATE_FORMAT(`finish_date`,'%Y-%m') as finish_month,
count(*) as number_app_runs
from metrics.user_app_usage ua inner join metrics.user_info ui
on ua.username = ui.username
where ui.kb_internal_user = False
and ui.exclude = False
and is_error = True
group by finish_month;

#IN METRICS_REPORTING
create or replace view metrics_reporting.app_usage_stats_by_month as
select apm.finish_month, apm.number_app_runs as num_all_app_runs,
nkapm.number_app_runs as non_kbase_user_app_runs,
aepm.number_app_runs as num_all_error_app_runs,
nkaepm.number_app_runs as non_kbase_user_error_app_runs,
apm.avg_queue_time
from metrics.hv_app_usage_by_month apm
left outer join metrics.hv_non_kbase_users_app_usage_by_month nkapm
on apm.finish_month = nkapm.finish_month
left outer join metrics.hv_app_usage_errors_by_month aepm
on apm.finish_month = aepm.finish_month
left outer join metrics.hv_non_kbase_users_app_usage_errors_by_month nkaepm
on apm.finish_month = nkaepm.finish_month;


------------------------------

#Bens Reports 1a
#Total number of users per institution;

#IN METRICS
create or replace view metrics.hv_users_per_institution as 
select count(*) as user_cnt, IFNULL(institution,'not specified') as institution
from metrics.user_info
where exclude = False 
group by institution;


#IN METRICS
create or replace view metrics.hv_non_kb_users_per_institution as 
select count(*) as user_cnt,  IFNULL(institution,'not specified') as institution
from metrics.user_info 
where kb_internal_user = False
and exclude = False  
group by institution;


#IN METRICS_REPORTING
create or replace view metrics_reporting.users_per_institution as
select upi.institution as institution, 
nkpi.user_cnt as non_kb_internal_users_cnt , 
upi.user_cnt as total_users_cnt
from metrics.hv_users_per_institution upi left outer join
metrics.hv_non_kb_users_per_institution nkpi on
upi.institution = nkpi.institution;

----------------------------------
#Bens Reports 1b
#Total number of users per institution over time.

#IN METRICS
create or replace view metrics.hv_users_per_institution_by_signup_month as 
select count(*) as user_cnt, 
DATE_FORMAT(`signup_date`,'%Y-%m') as signup_month,
IFNULL(institution,'not specified') as institution
from metrics.user_info 
where exclude = False 
group by institution, signup_month;

#IN METRICS
create or replace view metrics.hv_non_kb_users_per_institution_by_signup_month as 
select count(*) as user_cnt, 
DATE_FORMAT(`signup_date`,'%Y-%m') as signup_month,
IFNULL(institution,'not specified') as institution
from metrics.user_info 
where kb_internal_user = False
and exclude = False  
group by institution, signup_month;

#IN METRICS_REPORTING
create or replace view metrics_reporting.users_per_institution_by_signup_month as
select upi.institution as institution, 
upi.signup_month,
nkpi.user_cnt as non_kb_internal_users_cnt , 
upi.user_cnt as total_users_cnt
from metrics.hv_users_per_institution_by_signup_month upi left outer join
metrics.hv_non_kb_users_per_institution_by_signup_month nkpi on
upi.institution = nkpi.institution and
upi.signup_month = nkpi.signup_month;

--------------------------------------------

#Bens Reports 2a
#Total number of app runs by category.

#IN METRICS
create or replace view metrics.hv_all_app_category_run_counts as
select count(*) as total_app_run_cnt, 
IFNULL(acm.app_category,'unable to determine') as app_category
from metrics.user_app_usage uau
left outer join
metrics.app_name_category_map acm
on IFNULL(uau.app_name,'not specified') = acm.app_name
group by app_category;

#IN METRICS
create or replace view metrics.hv_non_kb_internal_app_category_run_counts as
select count(*) as non_kb_internal_app_run_cnt, 
IFNULL(acm.app_category,'unable to determine') as app_category
from metrics.user_info ui 
inner join 
metrics.user_app_usage uau 
on ui.username = uau.username
left outer join 
metrics.app_name_category_map acm
on IFNULL(uau.app_name,'not specified') = acm.app_name
where ui.kb_internal_user = False
and ui.exclude = False
group by app_category;

#IN METRICS_REPORTING - THIS IS A TABLE THAT GETS MADE ONCE A DAY BY CRON JOB: source/make_reporting_tables.py
create or replace table metrics_reporting.app_category_run_counts as
select aac.app_category, aac.total_app_run_cnt, nkbc.non_kb_internal_app_run_cnt
from metrics.hv_all_app_category_run_counts aac
left outer join
metrics.hv_non_kb_internal_app_category_run_counts nkbc
on aac.app_category = nkbc.app_category;

----------------------------------------------------
#Bens Reports 2b
#Total number of app runs by category and username

#IN METRICS_REPORTING
create or replace view metrics_reporting.app_category_run_counts_by_user as
select count(*) as total_app_run_cnt, 
IFNULL(acm.app_category,'unable to determine') as app_category,
uau.username, ui.user_id, ui.kb_internal_user
from metrics.user_info ui 
inner join metrics.user_app_usage uau
on ui.username = uau.username
left outer join
metrics.app_name_category_map acm
on IFNULL(uau.app_name,'not specified') = acm.app_name
where ui.exclude = False
group by app_category, uau.username, ui.kb_internal_user
order by app_category, total_app_run_cnt desc;

--------------------------------------------------

#Bens Reports 3a
#Total number of app runs by app_name.

#IN METRICS
create or replace view metrics.hv_all_app_name_run_counts as
select count(*) as total_app_run_cnt, 
IFNULL(app_name,'not specified') as app_name, func_name
from metrics.user_app_usage uau
group by app_name, func_name;

#IN METRICS
create or replace view metrics.hv_non_kb_internal_app_name_run_counts as
select count(*) as non_kb_internal_app_run_cnt, 
IFNULL(app_name,'not specified') as app_name, func_name
from metrics.user_app_usage uau
inner join metrics.user_info ui
on uau.username = ui.username
where ui.kb_internal_user = FALSE
and ui.exclude = False
group by app_name, func_name;

#IN METRICS_REPORTING
create or replace view metrics_reporting.app_name_run_counts as
select aac.app_name, aac.func_name, 
aac.total_app_run_cnt, IFNULL(non_kb_internal_app_run_cnt,0)
from metrics.hv_all_app_name_run_counts aac
left outer join metrics.hv_non_kb_internal_app_name_run_counts nkac
on aac.app_name = nkac.app_name
and aac.func_name = nkac.func_name;

-------------------------------------------
#Bens Reports 3b
#Total number of app runs by app_name and user.

#IN METRICS_REPORTING
create or replace view metrics_reporting.app_name_run_counts_by_user as
select count(*) as total_app_run_cnt, 
IFNULL(uau.app_name,'not specified') as app_name,
uau.func_name,
uau.username, ui.user_id, ui.kb_internal_user
from metrics.user_info ui 
inner join metrics.user_app_usage uau
on ui.username = uau.username
where ui.exclude = False
group by uau.app_name, uau.func_name, uau.username, ui.kb_internal_user
order by uau.app_name, uau.func_name, total_app_run_cnt desc;

-----------------------------------------------
#Bens report 4
#Total app runs for each app category per institution over time


#IN METRICS_REPORTING - THIS IS A TABLE THAT GETS MADE ONCE A DAY BY CRON JOB: source/make_reporting_tables.py
create or replace table metrics_reporting.institution_app_cat_run_counts as
select ui.institution, 
IFNULL(acm.app_category,'unable to determine') as app_category,
DATE_FORMAT(`finish_date`,'%Y-%m') as app_run_month,
count(*) as app_category_run_cnt
from metrics.user_info ui 
inner join 
metrics.user_app_usage uau 
on ui.username = uau.username
left outer join 
metrics.app_name_category_map acm
on IFNULL(uau.app_name,'not specified') = acm.app_name
where ui.exclude = False
group by ui.institution, app_category, app_run_month;


---------------------------------------------
# File Storage Data (SHOCK)

#IN METRICS
create or replace view metrics.hv_non_kbuser_monthly_file_stats as
select DATE_FORMAT(`record_date`,'%Y-%m') as month, 
sum(total_size) as total_size, sum(file_count) as file_count
from metrics.file_storage_stats fss
inner join metrics.user_info ui on ui.username = fss.username
where ui.kb_internal_user = False
group by month;

#IN METRICS
create or replace view metrics.hv_kbuser_monthly_file_stats as
select DATE_FORMAT(`record_date`,'%Y-%m') as month, 
sum(total_size) as total_size, sum(file_count) as file_count
from metrics.file_storage_stats fss
inner join metrics.user_info ui on ui.username = fss.username
where ui.kb_internal_user = True
group by month;

#IN METRICS_REPORTING
create or replace view metrics_reporting.monthly_file_stats as
select kufs.month as month, 
IFNULL(nkfs.total_size,0) as non_kbstaff_total_size,
IFNULL(kufs.total_size,0) as kbstaff_total_size,
IFNULL(nkfs.file_count,0) as non_kbstaff_file_count,
IFNULL(kufs.file_count,0) as kbstaff_file_count
from metrics.hv_kbuser_monthly_file_stats kufs
left outer join metrics.hv_non_kbuser_monthly_file_stats nkfs
on kufs.month = nkfs.month;

--------------------------------------------
#NEW APPS BEING RUN THE FIRST TIME (monthly counts)
# IDEA OF NEW APPS BEING RELEASED/USED

#IN METRICS
create or replace view metrics.hv_new_apps_first_run as
(select func_name, min(finish_date) as first_run
from metrics.user_app_usage
group by func_name);

#IN METRICS_REPORTING
create or replace view metrics_reporting.new_apps_first_run_month as
(select DATE_FORMAT(`first_run`,'%Y-%m') as first_run_month, count(*)
from metrics.hv_new_apps_first_run
group by first_run_month);

--------------------------------------------
#NEW APPS GIT COMMIT HASH BEING RUN THE FIRST TIME (monthly counts)
# IDEA OF NEW APPS GIT COMMITS BEING RELEASED/USED

#IN METRICS
create or replace view metrics.hv_new_apps_git_commit_first_run as
(select func_name, git_commit_hash, min(finish_date) as first_run
from metrics.user_app_usage
group by func_name, git_commit_hash);

#IN METRICS_REPORTING
create or replace view metrics_reporting.new_apps_git_commit_first_run_month as
select DATE_FORMAT(`first_run`,'%Y-%m') as first_run_month, count(*)
from metrics.hv_new_apps_git_commit_first_run
group by first_run_month;


---------------------------
#WORKSPACES MOST RECENT SNAPSHOT

#IN METRICS
CREATE OR REPLACE VIEW metrics.hv_workspaces_max_date as
select max(record_date) as record_date, ws_id
from metrics.workspaces w
group by ws_id;

#IN METRICS_REPORTING
CREATE OR REPLACE VIEW metrics_reporting.workspaces_current as
select ws.*
from metrics.workspaces ws inner join 
metrics.hv_workspaces_max_date wsmd
on ws.ws_id = wsmd.ws_id and 
ws.record_date = wsmd.record_date;


---------------------------
#WORKSPACES OBJECTS MOST RECENT SNAPSHOT

#IN METRICS
CREATE OR REPLACE VIEW metrics.hv_workspace_object_counts_max_date as
select max(record_date) as record_date, object_type_full
from metrics.workspace_object_counts
group by object_type_full;

#IN METRICS_REPORTING
CREATE OR REPLACE VIEW metrics_reporting.workspace_object_counts_current as
select wsoc.*
from metrics.workspace_object_counts wsoc inner join 
metrics.hv_workspace_object_counts_max_date wsmd
on wsoc.object_type_full = wsmd.object_type_full and 
wsoc.record_date = wsmd.record_date;


---------------------------
#USERS WORKSPACES OBJECTS MOST RECENT SNAPSHOT

#IN METRICS
CREATE OR REPLACE VIEW metrics.hv_users_workspace_object_counts_max_date as
select max(record_date) as record_date, object_type_full
from metrics.users_workspace_object_counts
group by object_type_full;

#IN METRICS_REPORTING
CREATE OR REPLACE VIEW metrics_reporting.users_workspace_object_counts_current as
select wsoc.*
from metrics.users_workspace_object_counts wsoc inner join 
metrics.hv_users_workspace_object_counts_max_date wsmd
on wsoc.object_type_full = wsmd.object_type_full and 
wsoc.record_date = wsmd.record_date;



---------------------------------
#USERS NARRATIVE COUNTS BY MONTH
#IN METRICS_REPORTING
CREATE OR REPLACE view metrics_reporting.users_narrative_counts_by_month as
select DATE_FORMAT(`initial_save_date`,'%Y-%m') as narrative_creation_month,
count(*) as users_narrative_count
from metrics_reporting.workspaces_current ws inner join 
metrics.user_info ui on ws.username = ui.username
where ui.kb_internal_user = 0
and ws.is_temporary = 0
and is_deleted = 0
group by narrative_creation_month;

-----------------------------
#KBase staff NARRATIVE COUNTS BY MONTH
#IN METRICS_REPORTING
CREATE OR REPLACE view metrics_reporting.kb_staff_narrative_counts_by_month as
select DATE_FORMAT(`initial_save_date`,'%Y-%m') as narrative_creation_month,
count(*) as kb_staff_narrative_count
from metrics_reporting.workspaces_current ws inner join 
metrics.user_info ui on ws.username = ui.username
where ui.kb_internal_user = 1
and ws.is_temporary = 0
and is_deleted = 0
group by narrative_creation_month;


-----------------------------
#ALL NARRATIVE COUNTS BY MONTH
#IN METRICS_REPORTING
CREATE OR REPLACE view metrics_reporting.all_narrative_counts_by_month as
select DATE_FORMAT(`initial_save_date`,'%Y-%m') as narrative_creation_month,
count(*) as all_narrative_count
from metrics_reporting.workspaces_current ws 
where ws.is_temporary = 0
and is_deleted = 0
group by narrative_creation_month;


---------------------------
#narrative_count_breakdowns_by_month
#IN METRICS_REPORTING
CREATE OR REPLACE view metrics_reporting.narrative_count_breakdowns_by_month as
select anc.narrative_creation_month,
anc.all_narrative_count, 
IFNULL(unc.users_narrative_count, 0) as users_narrative_count,
IFNULL(kbnc.kb_staff_narrative_count, 0) as kb_staff_narrative_count,
(IFNULL(unc.users_narrative_count, 0)/anc.all_narrative_count) * 100 as pct_users_narrative_counts
from metrics_reporting.all_narrative_counts_by_month anc 
left outer join metrics_reporting.users_narrative_counts_by_month unc on
anc.narrative_creation_month = unc.narrative_creation_month
left outer join metrics_reporting.kb_staff_narrative_counts_by_month kbnc on
anc.narrative_creation_month = kbnc.narrative_creation_month;



RUN TIME STATS OF ALL APPS SUCCESSFUL RUNS
-----------------------------
-------------------------------
#FUNCTION GIT RUN TIME STATS OF SUCCESSFUL RUNS
#IN METRICS_REPORTING
create or replace view metrics_reporting.function_git_combo_success_stats as
select func_name, git_commit_hash, 
count(run_time) as count, avg(run_time) as avg_run_time, 
max(run_time) as max_run_time, min(run_time) as min_run_time, 
stddev(run_time) as run_time_std_dev, 
max(finish_date) as max_finish_date,
min(finish_date) as min_finish_date,
round(avg(queue_time),1) as avg_queue_time,
min(queue_time) as min_queue_time,
max(queue_time) as max_queue_time
from metrics.user_app_usage 
where is_error = 0 
group by func_name, git_commit_hash;



-------------------------------
#FUNCTION RUN TIME STATS OF SUCCESSFUL RUNS
#IN METRICS_REPORTING
create or replace view metrics_reporting.function_success_stats as
select func_name,
count(run_time) as count, avg(run_time) as avg_run_time, 
max(run_time) as max_run_time, min(run_time) as min_run_time, 
stddev(run_time) as run_time_std_dev, 
max(finish_date) as max_finish_date,
min(finish_date) as min_finish_date,
round(avg(queue_time),1) as avg_queue_time,
min(queue_time) as min_queue_time,
max(queue_time) as max_queue_time
from metrics.user_app_usage 
where is_error = 0 
group by func_name;


----------------------------------
# Workspaces monthly sums
#IN METRICS_REPORTING
create or replace view metrics_reporting.workspaces_monthly_sums as
select DATE_FORMAT(`record_date`,'%Y-%m') as record_month,
count(*) as total_workspaces_count,
sum(top_lvl_object_count) as top_lvl_object_count,
sum(total_object_count) as total_object_count,
sum(visible_app_cells_count) as visible_app_cells_count,
sum(hidden_object_count) as hidden_object_count,
sum(deleted_object_count) as deleted_object_count,
sum(top_lvl_size) as top_lvl_size,
sum(total_size) as total_size,
sum(is_public) as public_workspace_count,
sum(is_temporary) as temporary_workspace_count,
sum(is_deleted) as deleted_workspace_count,
sum(number_of_shares) as total_number_of_shares,
sum(if(number_of_shares > 0, 1, 0)) as workspaces_with_shares
from metrics.workspaces
group by record_month;


----------------------------------
# USERS Narratives monthly sums
#IN METRICS_REPORTING

create or replace view metrics_reporting.users_narratives_monthly_sums as
select DATE_FORMAT(`record_date`,'%Y-%m') as record_month,
count(*) as total_narratives_count,
sum(top_lvl_object_count) as top_lvl_object_count,
sum(total_object_count) as total_object_count,
sum(visible_app_cells_count) as visible_app_cells_count,
sum(if(visible_app_cells_count > 0, 1, 0)) as narratives_with_visible_app_cells,
avg(visible_app_cells_count) as average_num_of_visible_app_cells_count,
stddev(visible_app_cells_count) as stddev_num_of_visible_app_cells_count,
max(visible_app_cells_count) as max_num_of_visible_app_cells_count,
(sum(if(visible_app_cells_count > 0, 1, 0))/count(*)) * 100 as pct_narratives_with_app_cells,
sum(hidden_object_count) as hidden_object_count,
sum(deleted_object_count) as deleted_object_count,
sum(top_lvl_size) as top_lvl_size,
sum(total_size) as total_size,
sum(is_public) as public_narratives_count,
sum(number_of_shares) as total_number_of_shares,
sum(if(number_of_shares > 0, 1, 0)) as narratives_with_shares
from metrics.workspaces ws inner join
metrics.user_info ui on ws.username = ui.username
where is_temporary = 0
and is_deleted = 0
and ui.kb_internal_user = 0
group by record_month;


-----------------------------------------
#KBStaff Narratives Monthly Sums
#IN METRICS_REPORTING

create or replace view metrics_reporting.kbstaff_narratives_monthly_sums as
select DATE_FORMAT(`record_date`,'%Y-%m') as record_month,
count(*) as total_narratives_count,
sum(top_lvl_object_count) as top_lvl_object_count,
sum(total_object_count) as total_object_count,
sum(visible_app_cells_count) as visible_app_cells_count,
sum(if(visible_app_cells_count > 0, 1, 0)) as narratives_with_visible_app_cells,
avg(visible_app_cells_count) as average_num_of_visible_app_cells_count,
stddev(visible_app_cells_count) as stddev_num_of_visible_app_cells_count,
max(visible_app_cells_count) as max_num_of_visible_app_cells_count,
(sum(if(visible_app_cells_count > 0, 1, 0))/count(*)) * 100 as pct_narratives_with_app_cells,
sum(hidden_object_count) as hidden_object_count,
sum(deleted_object_count) as deleted_object_count,
sum(top_lvl_size) as top_lvl_size,
sum(total_size) as total_size,
sum(is_public) as public_narratives_count,
sum(number_of_shares) as total_number_of_shares,
sum(if(number_of_shares > 0, 1, 0)) as narratives_with_shares
from metrics.workspaces ws inner join
metrics.user_info ui on ws.username = ui.username
where is_temporary = 0
and is_deleted = 0
and ui.kb_internal_user = 1
group by record_month;


-----------------------------------
# Object_counts over time
#IN METRICS_REPORTING
create or replace view metrics_reporting.workspace_object_counts as
select DATE_FORMAT(`record_date`,'%Y-%m') as record_month,
object_type, object_type_full, max(last_mod_date) as last_mod_date, 
sum(top_lvl_object_count) as top_lvl_object_count,
sum(total_object_count) as total_object_count,
sum(public_object_count) as public_object_count,
sum(private_object_count) as private_object_count,
sum(hidden_object_count) as hidden_object_count,
sum(deleted_object_count) as deleted_object_count,
sum(copy_count) as copy_count,
sum(top_lvl_size) as top_lvl_size,
sum(total_size) as total_size,
max(max_object_size) as max_object_size,
sum(total_size/total_object_count) as avg_object_size
from metrics.workspace_object_counts
group by record_month, object_type, object_type_full;


-----------------------------------
# Users_object_counts over time.
#IN METRICS_REPORTING
create or replace view metrics_reporting.users_workspace_object_counts as
select DATE_FORMAT(`record_date`,'%Y-%m') as record_month,
object_type, object_type_full, max(last_mod_date) as last_mod_date, 
sum(top_lvl_object_count) as top_lvl_object_count,
sum(total_object_count) as total_object_count,
sum(public_object_count) as public_object_count,
sum(private_object_count) as private_object_count,
sum(hidden_object_count) as hidden_object_count,
sum(deleted_object_count) as deleted_object_count,
sum(copy_count) as copy_count,
sum(top_lvl_size) as top_lvl_size,
sum(total_size) as total_size,
max(max_object_size) as max_object_size,
sum(total_size/total_object_count) as avg_object_size
from metrics.users_workspace_object_counts
group by record_month, object_type, object_type_full;


------------------
# Object_types over time (group by type)
#IN METRICS_REPORTING
create or replace view metrics_reporting.workspace_object_type_counts as
select DATE_FORMAT(`record_date`,'%Y-%m') as record_month,
object_type, max(last_mod_date) as last_mod_date, 
sum(top_lvl_object_count) as top_lvl_object_count,
sum(total_object_count) as total_object_count,
sum(public_object_count) as public_object_count,
sum(private_object_count) as private_object_count,
sum(hidden_object_count) as hidden_object_count,
sum(deleted_object_count) as deleted_object_count,
sum(copy_count) as copy_count,
sum(top_lvl_size) as top_lvl_size,
sum(total_size) as total_size,
max(max_object_size) as max_object_size,
sum(total_size/total_object_count) as avg_object_size
from metrics.workspace_object_counts
group by record_month, object_type;


-------------------
# USER Object_types over time (group by type)
#IN METRICS_REPORTING
create or replace view metrics_reporting.users_workspace_object_type_counts as
select DATE_FORMAT(`record_date`,'%Y-%m') as record_month,
object_type, max(last_mod_date) as last_mod_date, 
sum(top_lvl_object_count) as top_lvl_object_count,
sum(total_object_count) as total_object_count,
sum(public_object_count) as public_object_count,
sum(private_object_count) as private_object_count,
sum(hidden_object_count) as hidden_object_count,
sum(deleted_object_count) as deleted_object_count,
sum(copy_count) as copy_count,
sum(top_lvl_size) as top_lvl_size,
sum(total_size) as total_size,
max(max_object_size) as max_object_size,
sum(total_size/total_object_count) as avg_object_size
from metrics.users_workspace_object_counts
group by record_month, object_type;


---------------------
# USER CODE CELLS COUNTS AND DISTRIBUTIONS
#IN MEtrics Reporting
create or replace view metrics_reporting.user_code_cell_counts as
select wc.username, ui.user_id,
sum(code_cells_count) as user_code_cells_count
from metrics_reporting.workspaces_current wc
inner join metrics.user_info ui on ui.username = wc.username
where ui.kb_internal_user = 0
group by wc.username;

create or replace view metrics_reporting.user_code_cell_count_distribution as
select user_code_cells_count, count(*) as user_count
from metrics_reporting.user_code_cell_counts
group by user_code_cells_count;

create or replace view metrics_reporting.user_narratives_code_cell_count_distribution as
select code_cells_count, count(*) as nar_count 
from metrics_reporting.workspaces_current wc 
inner join metrics.user_info ui on ui.username = wc.username
where ui.kb_internal_user = 0
and wc.narrative_version > 0
group by code_cells_count;

-----------------------
#APP RUNS BY USERS AND WORKSPACES
#IN METRICS REPORTING
create or replace view metrics_reporting.user_app_runs as
select uau.username, ui.user_id,
count(*) as app_runs_count
from metrics.user_app_usage uau
inner join metrics.user_info ui on ui.username = uau.username
where ui.kb_internal_user = 0
group by uau.username;

create or replace view metrics_reporting.user_app_runs_distribution as
select app_runs_count, count(*) as user_count
from metrics_reporting.user_app_runs
group by app_runs_count;

create or replace view metrics_reporting.workspace_user_app_runs as
select uau.ws_id, count(*) as app_runs_count
from metrics.user_app_usage uau
inner join metrics.user_info ui on ui.username = uau.username
where ui.kb_internal_user = 0
group by uau.ws_id;

create or replace view metrics_reporting.workspaces_user_app_runs_distribution as
select app_runs_count, count(*) as ws_count
from metrics_reporting.workspace_user_app_runs
group by app_runs_count;

----------------------------

#------------------------------
# USER ORCID COUNT VIEWS.

#IN METRICS_REPORTING
create or replace view metrics_reporting.user_orcid_count_daily as
select 
DATE_FORMAT(`record_date`,'%Y-%m-%d') as date_daily,
max(user_orcid_count) as max_user_orcid_count
from metrics.user_orcid_count
group by date_daily;

#IN METRICS_REPORTING
create or replace view metrics_reporting.user_orcid_count_weekly as
select 
concat(substring(YEARWEEK(record_date),1,4),"-",substring(YEARWEEK(record_date),5,2)) as date_weekly,
max(user_orcid_count) as max_user_orcid_count
from metrics.user_orcid_count
group by date_weekly;

#IN METRICS_REPORTING
create or replace view metrics_reporting.user_orcid_count_monthly as
select 
DATE_FORMAT(`record_date`,'%Y-%m') as date_monthly,
max(user_orcid_count) as max_user_orcid_count
from metrics.user_orcid_count
group by date_monthly;


#----------------------------------
# Weekly App Category Users
# NOTE THESE ARE TABLES NOT VIEWS, made by the CRON JOB

create or replace table metrics.hv_weekly_app_category_unique_users as
select distinct DATE_FORMAT(`finish_date`,'%Y-%u') as week_run, 
IFNULL(app_category,'None') as app_category, uau.username
from metrics.user_app_usage uau inner join 
metrics.user_info ui on uau.username = ui.username
left outer join
metrics.app_name_category_map anc on uau.app_name = anc.app_name
where ui.kb_internal_user = 0
and func_name != 'kb_gtdbtk/run_kb_gtdbtk';

create or replace table metrics_reporting.app_category_unique_users_weekly as
select week_run, app_category, count(*) as unique_users
from metrics.hv_app_category_unique_users_weekly
group by week_run, app_category;


#------------------------------
# App reserved cpus for success and failures.
#

create view metrics_reporting.app_reserved_cpu_success as
select func_name, DATE_FORMAT(`finish_date`,'%Y-%m') as finish_month,
count(*) as run_count, 
round(avg(run_time),1) as avg_run_time_secs, round((sum(run_time)/3600) * reserved_cpu,1) as total_reserved_cpu_hours
from metrics.user_app_usage 
where is_error = 0
group by func_name, finish_month;

create view metrics_reporting.app_reserved_cpu_failure as
select func_name, DATE_FORMAT(`finish_date`,'%Y-%m') as finish_month,
count(*) as run_count, 
round(avg(run_time),1) as avg_run_time_secs, round((sum(run_time)/3600) * reserved_cpu,1) as total_reserved_cpu_hours
from metrics.user_app_usage 
where is_error = 1
group by func_name, finish_month;

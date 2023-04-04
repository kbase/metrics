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
select ui.* , sifc.country as session_info_country,
round((UNIX_TIMESTAMP(ui.last_signin_date) - UNIX_TIMESTAMP(ui.signup_date))/86400,2) as days_signin_minus_signup,
ceil((UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(last_signin_date))/86400) as days_since_last_signin,
IFNULL(uac.total_app_count,0) as total_app_count,
IFNULL(uec.total_app_err_count,0) as total_app_err_count
from metrics.user_info ui 
left outer join 
metrics.hv_user_app_count uac on ui.username = uac.username
left outer join   
metrics.hv_user_app_error_count uec on ui.username = uec.username
left outer join
metrics_reporting.session_info_frequent_country sifc on ui.username = sifc.username
where exclude = False
order by signup_date;

#IN METRICS_REPORTING
create or replace view metrics_reporting.anonymize_user_info_plus as
select user_id, kb_internal_user, institution, country, signup_date, last_signin_date, 
days_signin_minus_signup, days_since_last_signin, total_app_count, total_app_err_count
from metrics_reporting.user_info_plus
where exclude = 0;


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

#NOTE THIS VIEW MUST BE MADE HAND AFTER AN RESTORING DB FROM BACKUP
#BECAUSE METRICS IS RELYING METRICS_REPORTING (WHICH WILL NOT EXIST YET)
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

#NOTE THIS VIEW MUST BE MADE HAND AFTER AN RESTORING DB FROM BACKUP
#BECAUSE METRICS IS RELYING METRICS_REPORTING (WHICH WILL NOT EXIST YET)
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

#NOTE THIS VIEW MUST BE MADE HAND AFTER AN RESTORING DB FROM BACKUP
#BECAUSE METRICS IS RELYING METRICS_REPORTING (WHICH WILL NOT EXIST YET)
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

# Blobstore Data (blobstore)

#IN METRICS
create or replace view metrics.hv_non_kbuser_monthly_blobstore_stats as
select DATE_FORMAT(`record_date`,'%Y-%m') as month,
sum(total_size) as total_size, sum(file_count) as file_count
from metrics.blobstore_stats bss
inner join metrics.user_info ui on ui.username = bss.username
where ui.kb_internal_user = False
group by month;

#IN METRICS
create or replace view metrics.hv_kbuser_monthly_blobstore_stats as
select DATE_FORMAT(`record_date`,'%Y-%m') as month,
sum(total_size) as total_size, sum(file_count) as file_count
from metrics.blobstore_stats bss
inner join metrics.user_info ui on ui.username = bss.username
where ui.kb_internal_user = True
group by month;


#IN METRICS_REPORTING
create or replace view metrics_reporting.monthly_blobstore_stats as
select kubs.month as month, 
IFNULL(nkbs.total_size,0) as non_kbstaff_total_size,
IFNULL(kubs.total_size,0) as kbstaff_total_size,
IFNULL(nkbs.file_count,0) as non_kbstaff_file_count,
IFNULL(kubs.file_count,0) as kbstaff_file_count
from metrics.hv_kbuser_monthly_blobstore_stats kubs
left outer join metrics.hv_non_kbuser_monthly_blobstore_stats nkbs
on kubs.month = nkbs.month;

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
(select DATE_FORMAT(`first_run`,'%Y-%m') as first_run_month, count(*) as app_count
from metrics.hv_new_apps_first_run
group by first_run_month);

#IN METRICS
create or replace view metrics.hv_new_apps_categories_first_run as
(select uau.app_name, IFNULL(app_category, "No Category Association") as app_cat, min(finish_date) as first_run
from metrics.user_app_usage uau left outer join
metrics.app_name_category_map anm on uau.app_name = anm.app_name
group by uau.app_name, app_cat);

#USERS ONLY VERSION:
#IN METRICS
create or replace view metrics.hv_new_apps_first_run_users_only as
(select func_name, min(finish_date) as first_run
from metrics.user_app_usage uau inner join 
metrics.user_info ui on uau.username = ui.username
where kb_internal_user = 0
group by func_name);

#IN METRICS_REPORTING
create or replace view metrics_reporting.new_apps_first_run_month_users_only as
(select DATE_FORMAT(`first_run`,'%Y-%m') as first_run_month, count(*) as app_count
from metrics.hv_new_apps_first_run_users_only
group by first_run_month);

#IN METRICS
create or replace view metrics.hv_new_apps_categories_first_run_users_only as
(select uau.app_name, IFNULL(app_category, "No Category Association") as app_cat, min(finish_date) as first_run
from metrics.user_app_usage uau inner join 
metrics.user_info ui on uau.username = ui.username
left outer join 
metrics.app_name_category_map anm on uau.app_name = anm.app_name
where kb_internal_user = 0
group by uau.app_name, app_cat);



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


#---------------------
# App_queue_times_by_month
#

create view metrics_reporting.app_queue_times_by_month as
select func_name, DATE_FORMAT(`finish_date`,'%Y-%m') as finish_month,
count(*) as run_count, 
round(avg(queue_time),1) as avg_queue_time_secs, round(sum(queue_time)/3600,1) as total_queue_time_hours
from metrics.user_app_usage 
group by func_name, finish_month;


#---------------------
# Users most common country from session info
#

create or replace view metrics_reporting.hv_session_info_user_country_count as
select count(*) as session_count, si.username, si.country_name
from metrics.session_info si
group by si.username, si.country_name;


create or replace view metrics_reporting.hv_session_info_user_max_country_count as
select max(siuccm.session_count) as msession_count, siuccm.username 
from metrics_reporting.hv_session_info_user_country_count siuccm 
group by siuccm.username;


create or replace view metrics_reporting.session_info_frequent_country as
select siucc.username, min(siucc.country_name) as country
from metrics_reporting.hv_session_info_user_country_count siucc
inner join 
metrics_reporting.hv_session_info_user_max_country_count siumcc
on siucc.session_count = siumcc.msession_count
and siucc.username = siumcc.username
group by siucc.username;


#--------------------------
# Custom Table  for Adam so he can look at app workflows of users.
# This is done by cron job for making reporting tables.
#--------------------------
create or replace table metrics_reporting.narrative_app_flows as
select uau.ws_id, uau.username, uau.app_name, uau.func_name, uau.start_date, uau.finish_date
from metrics.user_info ui
inner join metrics.user_app_usage uau
on ui.username = uau.username
inner join metrics_reporting.workspaces_current wc
on wc.ws_id = uau.ws_id
where ui.kb_internal_user = 0
and uau.is_error = 0
and wc.narrative_version > 0
order by ws_id, start_date;


#-------------------------------------------
# USERS_OBJECT_CHANGES over time
#-------------------------------------------
create or replace view metrics_reporting.users_object_changes as
select object_type, 
DATE_FORMAT(`record_date`,'%Y-%m') as record_month,
sum(total_object_count) as total_object_count, 
round(sum(total_size)/1000000000,4) as total_size_GB,
sum(top_lvl_object_count) as top_lvl_object_count, 
round(sum(top_lvl_size)/1000000000,4) as top_lvl_size_GB
from metrics.users_workspace_object_counts
group by object_type, record_month
order by object_type, record_month;

#********************************************************************************************************************************
#-------------------------------
#-------------------------------
# VIEWS RELATED TO USER_SUPER_SUMMARY
#-------------------------------
#------------------------------

#------------------------------
# User Session summaries for user_super_summary
#------------------------------
create or replace view metrics.hv_user_session_count_all_time as
select username, count(*) as session_count_all_time
from metrics.session_info group by username;

create or replace view metrics.hv_user_session_count_last_year as
select username, count(*) as session_count_last_year
from metrics.session_info 
where record_date >= (NOW() - INTERVAL 365 DAY)
group by username;

create or replace view metrics.hv_user_session_count_last_90 as
select username, count(*) as session_count_last_90
from metrics.session_info 
where record_date >= (NOW() - INTERVAL 90 DAY)
group by username;

create or replace view metrics.hv_user_session_count_last_30 as
select username, count(*) as session_count_last_30
from metrics.session_info 
where record_date >= (NOW() - INTERVAL 30 DAY)
group by username;

#------------------------------
# App stats for user_super_summary as well as user_app_counts and users_app_counts_periods
#------------------------------

# NEEDS A CRON JOB
create or replace table metrics.hv_user_app_summaries as 
select username, 
min(finish_date) as first_app_run, 
max(finish_date) as last_app_run,  
count(*) as total_app_runs, 
sum(is_error) as total_error_runs, 
sum(run_time)/(3600) as total_run_time_hours, 
sum(queue_time)/(3600) as total_queue_time_hours, 
sum(reserved_cpu * run_time)/(3600) as total_CPU_hours 
from metrics.user_app_usage 
group by username;  

# NEEDS A CRON JOB
create or replace table metrics.hv_user_app_counts as 
select func_name, username, 
count(*) as user_app_count, 
sum(is_error) as user_error_count, 
min(finish_date) as first_app_run, 
max(finish_date) as last_app_run
from metrics.user_app_usage 
group by  func_name, username;
 
create or replace view metrics_reporting.user_app_counts as 
select * from metrics.hv_user_app_counts; 

create or replace view metrics.hv_user_max_used_app_count as 
select  uac.username, max(uac.user_app_count) as user_app_count 
from metrics.hv_user_app_counts uac 
group by uac.username; 

create or replace view metrics.hv_user_most_used_app as 
select uac.username, min(uac.func_name) as mu_func_name 
from metrics.hv_user_app_counts uac 
inner join metrics.hv_user_max_used_app_count umuac 
on uac.username = umuac.username 
and uac.user_app_count = umuac.user_app_count 
group by uac.username; 


create or replace view metrics.hv_users_distinct_apps_used_count as 
select username, count(*) as distinct_apps_used 
from metrics.hv_user_app_counts 
group by username; 

# NEEDS A CRON JOB
create or replace table metrics.hv_users_alltime_app_counts as 
select username, count(*) as app_count_all_time 
from metrics.user_app_usage uau_all 
group by username; 

# NEEDS A CRON JOB
create or replace table metrics.hv_users_last365days_app_counts as 
select username, count(*) as app_count_last_365 
from metrics.user_app_usage uau_365 
where finish_date >= (NOW() - INTERVAL 365 DAY) 
group by username; 

# NEEDS A CRON JOB
create or replace table metrics.hv_users_last_90days_app_counts as 
select username, count(*) as app_count_last_90 
from metrics.user_app_usage uau_90 
where finish_date >= (NOW() - INTERVAL 90 DAY) 
group by username; 

# NEEDS A CRON JOB
create or replace table metrics.hv_users_last_30days_app_counts as 
select username, count(*) as app_count_last_30 
from metrics.user_app_usage uau_30 
where finish_date >= (NOW() - INTERVAL 30 DAY) 
group by username; 

create or replace view metrics_reporting.users_app_counts_periods as 
select uac_all.username, sum(uac_all.app_count_all_time) as total_apps_run_all_time, 
sum(uac_365.app_count_last_365) as total_apps_run_last365, 
sum(uac_90.app_count_last_90) as total_apps_run_last90, 
sum(uac_30.app_count_last_30) as total_apps_run_last30 
from metrics.user_info ui 
inner join metrics.hv_users_alltime_app_counts uac_all 
on ui.username = uac_all.username 
left outer join metrics.hv_users_last365days_app_counts uac_365 
on ui.username = uac_365.username 
left outer join metrics.hv_users_last_90days_app_counts uac_90 
on ui.username = uac_90.username 
left outer join metrics.hv_users_last_30days_app_counts uac_30 
on ui.username = uac_30.username 
group by uac_all.username; 

#------------------------------
# Blob store summary stats for user_super_summary
#------------------------------
create or replace view metrics.hv_blobstore_user_summaries as 
select username, 
min(record_date) as first_file_date, 
max(record_date) as last_file_date, 
sum(total_size/1000000) as total_file_sizes_MB, 
sum(file_count) as total_file_count 
from blobstore_stats 
group by username; 

#------------------------------
# Narrative summaries - Will need to be done in a CRON job.
#------------------------------

# NEEDS A CRON JOB
create or replace table metrics_reporting.users_narratives_summary as 
select wc.username, 
ui.kb_internal_user, 
min(initial_save_date) as first_narrative_made_date, 
max(initial_save_date) as last_narrative_made_date,
max(mod_date) as last_narrative_modified_date,
sum(total_object_count) as total_narrative_objects_count, 
sum(top_lvl_object_count) as top_lvl_narrative_objects_count, 
sum(total_size) as total_narrative_objects_size, 
sum(top_lvl_size) as top_lvl_narrative_objects_size, 
count(*) as total_narrative_count, 
sum(is_public) as total_public_narrative_count, 
sum(ceiling(static_narratives_count/(static_narratives_count + .00000000000000000000001))) as distinct_static_narratives_count, 
sum(static_narratives_count) as static_narratives_created_count, 
sum(visible_app_cells_count) as total_visible_app_cells, 
sum(code_cells_count) as total_code_cells_count 
from metrics_reporting.workspaces_current wc 
inner join metrics.user_info ui 
on wc.username = ui.username 
where narrative_version > 0 
and is_deleted = 0 
and is_temporary = 0 
group by wc.username, ui.kb_internal_user; 

#------------------------------
Final user_super_summary table
#------------------------------

# NEEDS A CRON JOB
create or replace table metrics_reporting.user_super_summary as 
select uip.username, uip.display_name, 
uip.email, uip.kb_internal_user, uip.user_id, 
uip.globus_login, uip.google_login, uip.orcid, 
uip.session_info_country, uip.country, uip.state, 
uip.institution, uip.department, uip.job_title, 
uip.how_u_hear_selected, uip.how_u_hear_other,  
uip.signup_date, uip.last_signin_date, 
uip.days_signin_minus_signup,
uip.dev_token_first_seen,
days_since_last_signin, 
usssc.num_orgs, usssc.narrative_count,  
usssc.shared_count, usssc.narratives_shared, 
uns.first_narrative_made_date, uns.last_narrative_made_date, 
uns.last_narrative_modified_date,
uns.total_narrative_objects_count,uns.top_lvl_narrative_objects_count, 
uns.total_narrative_objects_size, uns.top_lvl_narrative_objects_size, 
uns.total_narrative_count, uns.total_public_narrative_count, 
uns.distinct_static_narratives_count, uns.static_narratives_created_count, 
uns.total_visible_app_cells, uns.total_code_cells_count, 
bus.first_file_date, bus.last_file_date, 
bus.total_file_sizes_MB, bus.total_file_count, 
umua.mu_func_name as most_used_app,  
udauc.distinct_apps_used, 
uapc.total_apps_run_all_time, uapc.total_apps_run_last365, 
uapc.total_apps_run_last90, uapc.total_apps_run_last30, 
uas.total_error_runs as total_app_errors_all_time, 
uas.first_app_run, uas.last_app_run,
uas.total_run_time_hours, uas.total_queue_time_hours, 
uas.total_CPU_hours, 
uscat.session_count_all_time, 
uscly.session_count_last_year, 
usc90.session_count_last_90, 
usc30.session_count_last_30
from metrics_reporting.user_info_plus uip
inner join metrics.user_system_summary_stats_current usssc 
on uip.username = usssc.username
left outer join metrics_reporting.users_narratives_summary uns
on uip.username = uns.username
left outer join metrics.hv_blobstore_user_summaries bus
on uip.username = bus.username
left outer join metrics_reporting.users_app_counts_periods uapc
on uip.username = uapc.username
left outer join metrics.hv_user_app_summaries uas
on uip.username = uas.username
left outer join metrics.hv_user_most_used_app umua 
on uip.username = umua.username 
left outer join metrics.hv_users_distinct_apps_used_count udauc 
on uip.username = udauc.username 
left outer join metrics.hv_user_session_count_all_time uscat
on uip.username = uscat.username
left outer join metrics.hv_user_session_count_last_year uscly
on uip.username = uscly.username
left outer join metrics.hv_user_session_count_last_90 usc90
on uip.username = usc90.username
left outer join metrics.hv_user_session_count_last_30 usc30
on uip.username = usc30.username
where uip.exclude != 1;

# END OF USER_SUPER_SUMMARY
#********************************************************************************************************************************

# public_narratives_app_use

create or replace view metrics_reporting.public_narratives_app_use as
select ui.username, ua.ws_id, DATE_FORMAT(`mod_date`,'%Y') as narrative_last_modified_year, 
finish_date, DATE_FORMAT(`finish_date`,'%Y') as app_year, 
DATE_FORMAT(`finish_date`,'%Y-%m') as app_month,
ua.app_name, ua.func_name
from metrics.user_info ui
inner join metrics_reporting.workspaces_current wc
on ui.username = wc.username
inner join metrics.user_app_usage ua
on ua.ws_id = wc.ws_id
where wc.is_public = 1
and wc.is_deleted = 0
and ui.kb_internal_user = 0
and ua.is_error = 0;


#*********************************************************
#
# DOI Metrics
CREATE OR REPLACE VIEW metrics_reporting.doi_metrics_current as
select pm.*
from (
select max(m_rdate.record_date) as maxdate, m_rdate.ws_id as max_rdate_ws_id
from metrics.doi_metrics m_rdate
group by m_rdate.ws_id) as max_date_ws_id
inner join metrics.doi_metrics pm
on pm.record_date = max_date_ws_id.maxdate
and pm.ws_id = max_date_ws_id.max_rdate_ws_id;

create or replace view metrics_reporting.unique_workspaces_with_doi_data as
select doi_ws_id, group_concat(derived_ws_id ORDER BY derived_ws_id ASC SEPARATOR ', ') as ws_ids_using_data
from metrics.doi_unique_workspaces
group by doi_ws_id;

create or replace view metrics_reporting.unique_usernames_with_doi_data as
select doi_ws_id, group_concat(derived_username ORDER BY derived_username ASC SEPARATOR ', ') as usernames_using_data
from metrics.doi_unique_usernames
group by doi_ws_id;

create or replace view metrics_reporting.doi_metrics_current_full as
select dwm.doi_url, dwm.ws_id, dwm.title, dwm.is_parent_ws,
pmc.unique_users_count, pmc.unique_ws_ids_count,
uup.usernames_using_data, uwp.ws_ids_using_data,
pmc.downloads_count, pmc.narrative_views_count, pmc.derived_object_count,
pmc.copied_only_object_count, pmc.fully_derived_object_pair_counts
from metrics.doi_ws_map dwm inner join
metrics_reporting.doi_metrics_current pmc
on dwm.ws_id = pmc.ws_id
left outer join metrics_reporting.unique_workspaces_with_doi_data uwp
on  pmc.ws_id = uwp.doi_ws_id
left outer join metrics_reporting.unique_usernames_with_doi_data uup
on  pmc.ws_id = uup.doi_ws_id
order by dwm.doi_url, is_parent_ws desc, dwm.ws_id;

create or replace view metrics.hv_doi_ws_with_children as
select ws_id
from metrics.doi_ws_map dwm inner join
(select doi_url, count(*) as children_count from metrics.doi_ws_map group by doi_url having children_count > 1) as inner_map
on dwm.doi_url = inner_map.doi_url
where dwm.is_parent_ws = 1;

CREATE or replace VIEW `metrics_reporting.doi_fully_derived_objects` AS
(select distinct `dido`.`doi_ws_id` AS `doi_ws_id`,`dido`.`doi_object_input_id` AS `doi_object_id`,
`dedo`.`derived_object_id` AS `derived_object_id`,`dedo`.`derived_object_owner` AS `derived_object_owner`,
`dedo`.`derived_object_ws_id` AS `derived_object_ws_id`,
case when `dido`.`steps_away` = 0 then `dedo`.`derived_is_copy_only` else 0 end AS `copied_only`
from (`metrics`.`doi_internally_derived_objects` `dido` join `metrics`.`doi_externally_derived_objects` `dedo` on(`dido`.`doi_object_output_id` = `dedo`.`doi_object_id`)));

CREATE or replace VIEW `metrics_reporting.doi_metrics_current_with_doi_info` AS
(select dwm.*, dmc.record_date, dmc.unique_users_count, dmc.unique_ws_ids_count,
dmc.derived_object_count, dmc.copied_only_object_count, dmc.fully_derived_object_pair_counts
from metrics.doi_ws_map dwm inner join metrics_reporting.doi_metrics_current dmc
on dwm.ws_id =dmc.ws_id
order by dwm.doi_url, is_parent_ws desc);


create or replace view metrics_reporting.doi_metrics_current_report
as (
select dwm.doi_url AS doi_url, dwm.title AS title, dwm.is_parent_ws AS is_parent_ws, 
dmc.ws_id AS ws_id, dmc.record_date AS record_date, dmc.unique_users_count AS unique_users_count, dmc.unique_ws_ids_count AS unique_ws_ids_count,
dmc.ttl_dls_cnt AS ttl_dls_cnt, dmc.ttl_uniq_dl_users_cnt AS ttl_uniq_dl_users_cnt, dmc.ttl_dl_user_doi_obj_cnt AS ttl_dl_user_doi_obj_cnt,
dmc.ttl_dl_users_dled_obj_cnt AS ttl_dl_users_dled_obj_cnt, dmc.derived_object_count AS derived_object_count,
dmc.copied_only_object_count AS copied_only_object_count, dmc.fully_derived_object_pair_counts AS fully_derived_object_pair_counts,
wc.static_narratives_views
from metrics.doi_ws_map dwm inner join 
metrics_reporting.doi_metrics_current dmc on dwm.ws_id = dmc.ws_id
inner join metrics_reporting.workspaces_current wc on dmc.ws_id = wc.ws_id
order by dwm.doi_url,dwm.is_parent_ws desc);

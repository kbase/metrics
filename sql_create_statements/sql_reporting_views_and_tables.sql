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
ui.kb_internal_user, ui.institution, ui.country,
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
func_name, git_commit_hash 
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
te.tot_cnt, te.app_name, te.func_name, te.git_commit_hash
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
nuu.all_users_count, non_kb_user_count
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
select count(*)as non_kb_user_count, func_name, git_commit_hash 
from metrics.hv_distinct_nonKB_users_using_func_git
group by func_name, git_commit_hash;

#IN METRICS
create or replace view metrics.hv_total_exec_function_git as
select count(*) as tot_cnt,  
func_name, git_commit_hash from metrics.user_app_usage
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
te.tot_cnt, te.func_name, te.git_commit_hash
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
nuu.all_users_count, non_kb_user_count
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
and ui.exclude = False;

#IN METRICS
create or replace view metrics.hv_number_nonKB_users_using_function as
select count(*)as non_kb_user_count, func_name
from metrics.hv_distinct_nonKB_users_using_func
group by func_name;

#IN METRICS
create or replace view metrics.hv_total_exec_function as
select count(*) as tot_cnt, func_name 
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
te.tot_cnt, te.func_name
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
nuu.all_users_count, non_kb_user_count
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
count(*) as number_app_runs
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
nkaepm.number_app_runs as non_kbase_user_error_app_runs
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
uau.username, ui.kb_internal_user
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
uau.username, ui.kb_internal_user
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



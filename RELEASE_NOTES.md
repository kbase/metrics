#2020-06-19
    * Added user_id to user info, backfilling and uploading

#2020-06-15
    * Added user orcid count tracking

#2020-06-09
    * Moved EE2 to active spot instead of app catalog. Created old_app_catalog_equivalent

#2020-05-27
    * Making code to use EE2 for app stats active

#2020-05-12
    * Changed sheet used for KBase staff list for user stats

#2020-04-23
    * Fixed Sonar bugs for undefined variables and made non-existant type of errors into generic exceptions

#2020-04-20 First creation of the file. Major reorg of directories. Hopefully easier to follow. There is now 3 major directories in source :
    * daily_cron_jobs
    * monthly_cron_jobs
    * custom_scripts (special scripts needed to create custom data reports for Adam, back fill data or determine data deiscrepencies)


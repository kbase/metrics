#2020-05-05
* Apps stats uploading set up for EE2 and grabs reserved CPU. Also made backfil script.


#2020-04-23
*Fixed Sonar bugs for undefined variables and made non-existant type of errors into generic exceptions 


#2020-04-20
First creation of the file.
Major reorg of directories. Hopefully easier to follow.
There is now 3 major directories in source :
* daily_cron_jobs
* monthly_cron_jobs
* custom_scripts  (special scripts needed to create custom data reports for Adam, back fill data or determine data deiscrepencies)
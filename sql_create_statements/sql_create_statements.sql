######################
# user_info table create and indices.

CREATE TABLE user_info (
	username VARCHAR(255) NOT NULL,  
	display_name VARCHAR(255) NOT NULL,  
	email VARCHAR(255),  
	orcid VARCHAR(255),
	kb_internal_user BOOLEAN NOT NULL DEFAULT 0,
	institution VARCHAR(255),
	country VARCHAR(255), 
	signup_date TIMESTAMP NOT NULL,
	last_signin_date TIMESTAMP NULL default NULL,
	PRIMARY KEY ( username )) ENGINE=InnoDB  DEFAULT CHARSET=utf8; 


CREATE INDEX idx_user_info_email ON user_info (email);

CREATE INDEX idx_user_info_orcid ON user_info (orcid);

CREATE INDEX idx_user_info_kbase_internal_user ON user_info (kb_internal_user);

CREATE INDEX idx_user_info_signup_date ON user_info (signup_date);

CREATE INDEX idx_user_info_last_signin_date ON user_info (last_signin_date);

CREATE INDEX idx_user_info_institution ON user_info (institution);


######################
# user_system_summary_stats table create, indices, unique constraint, and trigger

CREATE TABLE user_system_summary_stats (
	username VARCHAR(255) NOT NULL,  
	num_orgs INTEGER NOT NULL,
	narrative_count INTEGER NOT NULL,
	shared_count INTEGER NOT NULL,
	narratives_shared INTEGER NOT NULL,    
	record_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY fk_sys_narrative_username(username)
	REFERENCES user_info(username)
	ON UPDATE CASCADE
	ON DELETE RESTRICT) ENGINE=InnoDB DEFAULT CHARSET=utf8; 

#	record_date DATE,

CREATE UNIQUE INDEX uk_user_system_summary_stats_record_date
ON user_system_summary_stats(username,record_date);

CREATE INDEX idx_user_system_summary_stats_record_date ON user_system_summary_stats (record_date);


#PROBABLY DONT NEED THIS SWITCHING RECORD DATE TO TIMESTAMP I THINK
# Trigger that will put in current date for record date for any insert without a record_date (should be default insert behavior).
DELIMITER $$
CREATE TRIGGER `user_system_summary_stats_record_date_trigger` BEFORE INSERT ON `user_system_summary_stats` FOR EACH ROW
if ( isnull(new.record_date) ) then
 set new.record_date=curdate();
end if;
$$
delimiter ;

######################
#user_app_usage

CREATE TABLE user_app_usage (
        job_id VARCHAR(255) NOT NULL,
        username VARCHAR(255) NOT NULL,
        app_name VARCHAR(255) NOT NULL,
        start_date      TIMESTAMP NOT NULL,
        finish_date     TIMESTAMP NOT NULL,
        run_time INTEGER NOT NULL,
        is_error BOOLEAN NOT NULL DEFAULT 0,
	git_commit_hash VARCHAR(255) NOT NULL,
	PRIMARY KEY ( job_id ), 
        FOREIGN KEY fk_app_usage_username(username)
        REFERENCES user_info(username)
        ON UPDATE CASCADE
        ON DELETE RESTRICT) ENGINE=InnoDB DEFAULT CHARSET=utf8;



#WITH ADDITION OF JOB ID AS A PRIMARY KEY THIS IS NOT LONGER NEEDED
#CREATE UNIQUE INDEX uk_user_app_usage
#ON user_app_usage(username, app_name, 
#start_date,finish_date,run_time,is_error);

######################
#app_name_category_map

CREATE TABLE app_name_category_map (
	app_name VARCHAR(255) NOT NULL,
	app_category VARCHAR(255) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8; 

CREATE UNIQUE INDEX uk_app_name_category
ON app_name_category_map(app_name, app_category);

CREATE INDEX idx_app_name_category_map_app_name ON app_name_category_map (app_name);

CREATE INDEX idx_app_name_category_map_app_category ON app_name_category_map (app_category);







######################
CREATE VIEW user_system_max_date as 
select max(u_cdate.record_date) as maxdate, u_cdate.username
from user_system_summary_stats u_cdate
group by u_cdate.username;


CREATE VIEW user_system_summary_stats_current as  
select usss.*
from user_system_summary_stats usss,
user_system_max_date usmd
where usss.username = usmd.username
and usss.record_date = usmd.maxdate;


CREATE VIEW user_system_min_date as 
select min(u_cdate.record_date) as mindate, u_cdate.username
from user_system_summary_stats u_cdate
group by u_cdate.username;

CREATE VIEW user_system_summary_stats_first as  
select usss.*
from user_system_summary_stats usss,
user_system_min_date usmd
where usss.username = usmd.username
and usss.record_date = usmd.mindate;




######################
######################
######################
######################
# NOT DONE YET WE CAN DO IF WE UPGRADE MYSQL VERSION
#CURRENT SNAPSHOT
CREATE VIEW user_system_summary_stats_current as  
select usss.*
from user_system_summary_stats usss,
(select max(u_cdate.record_date) as maxdate, u_cdate.username
from user_system_summary_stats u_cdate
group by u_cdate.username) as user_date
where usss.username = user_date.username
and usss.record_date = user_date.maxdate;


#FIRST SNAPSHOT
CREATE VIEW user_system_summary_stats_first as  
select usss.*
from user_system_summary_stats usss,
(select min(u_fdate.record_date) as mindate, u_fdate.username
from user_system_summary_stats u_fdate
group by u_fdate.username) as user_date
where usss.username = user_date.username
and usss.record_date = user_date.mindate;

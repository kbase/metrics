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
	signup_date TIMESTAMP NOT NULL default 0,
	last_signin_date TIMESTAMP NULL default NULL,
	exclude boolean NOT NULL default 0, 
	PRIMARY KEY ( username )) ENGINE=InnoDB  DEFAULT CHARSET=utf8; 

CREATE INDEX idx_user_info_email ON user_info (email);

CREATE INDEX idx_user_info_orcid ON user_info (orcid);

CREATE INDEX idx_user_info_kbase_internal_user ON user_info (kb_internal_user);

CREATE INDEX idx_user_info_signup_date ON user_info (signup_date);

CREATE INDEX idx_user_info_last_signin_date ON user_info (last_signin_date);

CREATE INDEX idx_user_info_institution ON user_info (institution);

CREATE INDEX idx_user_info_country ON metrics.user_info (country);

CREATE INDEX idx_user_info_exclude ON metrics.user_info (exclude);



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

CREATE INDEX idx_user_system_summary_stats_username ON user_system_summary_stats (username);

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


CREATE TABLE `user_app_usage` (
  `job_id` varchar(255) DEFAULT NULL,
  `username` varchar(255) NOT NULL,
  `app_name` varchar(255) DEFAULT NULL,
  `start_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `finish_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `run_time` int(11) NOT NULL,
  `queue_time` int(11) NOT NULL,
  `is_error` tinyint(1) NOT NULL DEFAULT '0',
  `git_commit_hash` varchar(255) NOT NULL,
  `func_name` varchar(255) DEFAULT NULL,
  UNIQUE KEY `uk_jobid_user_app_usage` (`job_id`),
  KEY `idx_user_app_usage_job_id` (`job_id`),
  KEY `idx_user_app_usage_username` (`username`),
  KEY `idx_user_app_usage_app_name` (`app_name`),
  KEY `idx_user_app_usage_start_date` (`start_date`),
  KEY `idx_user_app_usage_finish_date` (`finish_date`),
  KEY `idx_user_app_usage_is_error` (`is_error`),
  KEY `idx_user_app_usage_git_commit_hash` (`git_commit_hash`),
  KEY `idx_user_app_usage_func_name` (`func_name`),
  CONSTRAINT `fk_app_usage_username` FOREIGN KEY (`username`) REFERENCES `user_info` (`username`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8


CREATE TABLE user_app_usage (
        job_id VARCHAR(255),
        username VARCHAR(255) NOT NULL,
        app_name VARCHAR(255),
        start_date      TIMESTAMP NOT NULL default 0,
        finish_date     TIMESTAMP NOT NULL default 0,
        run_time INTEGER NOT NULL,
        is_error BOOLEAN NOT NULL DEFAULT 0,
	git_commit_hash VARCHAR(255) NOT NULL,
	func_name VARCHAR(255),
        FOREIGN KEY fk_app_usage_username(username)
        REFERENCES user_info(username)
        ON UPDATE CASCADE
        ON DELETE RESTRICT) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE UNIQUE INDEX uk_jobid_user_app_usage
ON user_app_usage(job_id);

CREATE INDEX idx_user_app_usage_job_id ON metrics.user_app_usage (job_id);

CREATE INDEX idx_user_app_usage_username ON metrics.user_app_usage (username);

CREATE INDEX idx_user_app_usage_app_name ON metrics.user_app_usage (app_name);

CREATE INDEX idx_user_app_usage_start_date ON metrics.user_app_usage (start_date);

CREATE INDEX idx_user_app_usage_finish_date ON metrics.user_app_usage (finish_date);

CREATE INDEX idx_user_app_usage_is_error ON metrics.user_app_usage (is_error);

CREATE INDEX idx_user_app_usage_git_commit_hash ON metrics.user_app_usage (git_commit_hash);

CREATE INDEX idx_user_app_usage_func_name ON metrics.user_app_usage (func_name);

########
#EE2
CREATE TABLE `user_app_usage_ee2` (
  `job_id` varchar(255) DEFAULT NULL,
  `username` varchar(255) NOT NULL,
  `app_name` varchar(255) DEFAULT NULL,
  `start_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `finish_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `run_time` int(11) NOT NULL,
  `queue_time` int(11) NOT NULL,
  `is_error` tinyint(1) NOT NULL DEFAULT '0',
  `git_commit_hash` varchar(255) NOT NULL,
  `func_name` varchar(255) DEFAULT NULL,
  `ws_id` int(11) DEFAULT NULL,
  UNIQUE KEY `uk_jobid_user_app_usage_ee2` (`job_id`),
  KEY `idx_user_app_usage_job_id_ee2` (`job_id`),
  KEY `idx_user_app_usage_username_ee2` (`username`),
  KEY `idx_user_app_usage_app_name_ee2` (`app_name`),
  KEY `idx_user_app_usage_start_date_ee2` (`start_date`),
  KEY `idx_user_app_usage_finish_date_ee2` (`finish_date`),
  KEY `idx_user_app_usage_is_error_ee2` (`is_error`),
  KEY `idx_user_app_usage_git_commit_hash_ee2` (`git_commit_hash`),
  KEY `idx_user_app_usage_func_name_ee2` (`func_name`),
  KEY `idx_user_app_usage_ws_id_ee2` (`ws_id`),
  CONSTRAINT `fk_app_usage_username_ee2` FOREIGN KEY (`username`) REFERENCES `user_info` (`username`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8


#######
#EE2 with CPU
CREATE TABLE `user_app_usage_ee2_cpu` (
  `job_id` varchar(255) DEFAULT NULL,
  `username` varchar(255) NOT NULL,
  `app_name` varchar(255) DEFAULT NULL,
  `start_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `finish_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `run_time` int(11) NOT NULL,
  `queue_time` int(11) NOT NULL,
  `is_error` tinyint(1) NOT NULL DEFAULT '0',
  `git_commit_hash` varchar(255) NOT NULL,
  `func_name` varchar(255) DEFAULT NULL,
  `ws_id` int(11) DEFAULT NULL,
  `reserved_cpu` int(4) DEFAULT NULL,
  UNIQUE KEY `uk_jobid_user_app_usage_ee2_cpu` (`job_id`),
  KEY `idx_user_app_usage_job_id_ee2_cpu` (`job_id`),
  KEY `idx_user_app_usage_username_ee2_cpu` (`username`),
  KEY `idx_user_app_usage_app_name_ee2_cpu` (`app_name`),
  KEY `idx_user_app_usage_start_date_ee2_cpu` (`start_date`),
  KEY `idx_user_app_usage_finish_date_ee2_cpu` (`finish_date`),
  KEY `idx_user_app_usage_is_error_ee2_cpu` (`is_error`),
  KEY `idx_user_app_usage_git_commit_hash_ee2_cpu` (`git_commit_hash`),
  KEY `idx_user_app_usage_func_name_ee2_cpu` (`func_name`),
  KEY `idx_user_app_usage_ws_id_ee2_cpu` (`ws_id`),
  CONSTRAINT `fk_app_usage_username_ee2_cpu` FOREIGN KEY (`username`) REFERENCES `user_info` (`username`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8


######################
#app_name_category_map

CREATE TABLE app_name_category_map (
	app_name VARCHAR(255) NOT NULL,
	app_category VARCHAR(255) NOT NULL,
	is_active BOOLEAN NOT NULL DEFAULT 0 
	) ENGINE=InnoDB DEFAULT CHARSET=utf8; 

CREATE UNIQUE INDEX uk_app_name_category
ON app_name_category_map(app_name, app_category);

CREATE INDEX idx_app_name_category_map_app_name ON app_name_category_map (app_name);

CREATE INDEX idx_app_name_category_map_app_category ON app_name_category_map (app_category);


#########################
#public_narrative_count

CREATE TABLE public_narrative_count (
       public_narrative_count INTEGER NOT NULL,
       record_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE UNIQUE INDEX uk_public_narrative_count_pnc_record_date
ON public_narrative_count(public_narrative_count,record_date);



#########################
#session_info

CREATE or replace TABLE metrics.session_info (
        username VARCHAR(255) NOT NULL,
        record_date DATE NOT NULL,
        ip_address VARCHAR(15) NOT NULL,
        country_name VARCHAR(255) NOT NULL,
        country_code VARCHAR(3) NOT NULL,
        city VARCHAR(255) NOT NULL,
        latitude DECIMAL(6,4) DEFAULT NULL,
        longitude DECIMAL(7,4) DEFAULT NULL,
        region_name VARCHAR(255) NOT NULL,
        region_code VARCHAR(255) NOT NULL,
        postal_code VARCHAR(255) NOT NULL,
        timezone VARCHAR(255) NOT NULL,
        estimated_hrs_active DECIMAL(6,4) NOT NULL,
        first_seen TIMESTAMP NOT NULL default 0,
        last_seen TIMESTAMP NOT NULL default 0,
        proxy_target VARCHAR(30) NOT NULL,
        FOREIGN KEY fk_session_user_info_username(username)
        REFERENCES metrics.user_info(username)
        ON UPDATE CASCADE
        ON DELETE RESTRICT) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        
        
CREATE UNIQUE INDEX uk_session_user_date_ip
ON metrics.session_info(username,record_date,ip_address); 

CREATE INDEX idx_session_info_username ON metrics.session_info(username);

CREATE INDEX idx_session_info_record_date ON metrics.session_info(record_date);

CREATE INDEX idx_session_info_ip_address ON metrics.session_info(ip_address);

CREATE INDEX idx_session_info_county_name ON metrics.session_info(country_name);

CREATE INDEX idx_session_info_county_code ON metrics.session_info(country_code);

CREATE INDEX idx_session_info_city ON metrics.session_info(city);

CREATE INDEX idx_session_info_region_name ON metrics.session_info(region_name);

CREATE INDEX idx_session_info_region_code ON metrics.session_info(region_code);

CREATE INDEX idx_session_info_timezone ON metrics.session_info(timezone);

CREATE INDEX idx_session_info_estimated_hrs_active ON metrics.session_info(estimated_hrs_active);



################################################
# file_storage_stats

CREATE TABLE file_storage_stats (
    username VARCHAR(255) NOT NULL,
    record_date DATE NOT NULL,
    total_size BIGINT NOT NULL,
    file_count INT NOT NULL,
    FOREIGN KEY fk_sys_large_file_storage_stats_username(username)
    REFERENCES user_info(username)
        ON UPDATE CASCADE
    ON DELETE RESTRICT) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    
CREATE UNIQUE INDEX uk_file_storage_stats_user_date
ON metrics.file_storage_stats(username,record_date);

CREATE INDEX idx_file_storage_stats_username ON metrics.file_storage_stats(username);

CREATE INDEX idx_file_storage_stats_record_date ON metrics.file_storage_stats(record_date);

##################################################
# workspaces

CREATE OR REPLACE TABLE `workspaces` (
  `ws_id` int(11) NOT NULL,
  `username` varchar(255) NOT NULL,
  `mod_date` date NOT NULL,
  `initial_save_date` date default NULL,
  `record_date` date NOT NULL,
  `top_lvl_object_count` int(11) NOT NULL,
  `total_object_count` int(11) NOT NULL,
  `visible_app_cells_count` int(11) NOT NULL,
  `code_cells_count` int(11) NOT NULL,
  `narrative_version` int(11) NOT NULL DEFAULT '0',
  `hidden_object_count` int(11) NOT NULL DEFAULT '0',
  `deleted_object_count` int(11) NOT NULL DEFAULT '0',
  `total_size` bigint(20) NOT NULL,
  `top_lvl_size` bigint(20) NOT NULL,
  `is_public` tinyint(1) NOT NULL DEFAULT '0',
  `is_temporary` tinyint(1) DEFAULT NULL,
  `is_deleted` tinyint(1) NOT NULL DEFAULT '0',	
  `number_of_shares` int(11) NOT NULL DEFAULT '0',
  `num_nar_obj_ids` int(6) not null default 0,
  UNIQUE KEY `uk_ws_user_rd_workspaces` (`ws_id`,`username`,`record_date`),
  KEY `idx_workspaces_ws_id` (`ws_id`),
  KEY `idx_workspaces_user` (`username`),
  KEY `idx_workspaces_rd` (`record_date`),
  KEY `idx_workspaces_isd` (`initial_save_date`),
  KEY `idx_workspaces_tloc` (`top_lvl_object_count`),
  KEY `idx_workspaces_cc` (`code_cells_count`),
  KEY `idx_workspaces_vac` (`visible_app_cells_count`),
  KEY `idx_workspaces_nv` (`narrative_version`),
  KEY `idx_workspaces_ip` (`is_public`),
  KEY `idx_workspaces_it` (`is_temporary`),
  KEY `idx_workspaces_id` (`is_deleted`),
  CONSTRAINT `fk_workspace_narratives_username` FOREIGN KEY (`username`) REFERENCES `user_info` (`username`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


############################################
# workspace_object_types

CREATE OR REPLACE TABLE metrics.workspace_object_counts (
    object_type VARCHAR(255) NOT NULL,
    object_spec_version VARCHAR(255) NOT NULL,
    object_type_full VARCHAR(255) NOT NULL,
    record_date DATE NOT NULL,
    last_mod_date DATE NOT NULL,
    top_lvl_object_count INTEGER NOT NULL,
    total_object_count INTEGER NOT NULL,
    public_object_count INTEGER NOT NULL,
    private_object_count INTEGER NOT NULL,
    hidden_object_count INTEGER NOT NULL DEFAULT 0,
    deleted_object_count INTEGER NOT NULL DEFAULT 0,
    copy_count INTEGER NOT NULL,
    total_size BIGINT NOT NULL,
    top_lvl_size BIGINT NOT NULL,
    max_object_size BIGINT NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        
CREATE UNIQUE INDEX uk_type_ver_rd_workspace_object_counts
ON metrics.workspace_object_counts(object_type, object_spec_version, record_date);

CREATE INDEX idx_workspace_object_counts_ot ON metrics.workspace_object_counts (object_type);

CREATE INDEX idx_workspace_object_counts_osv ON metrics.workspace_object_counts (object_spec_version);

CREATE INDEX idx_workspace_object_counts_otf ON metrics.workspace_object_counts (object_type_full);

CREATE INDEX idx_workspace_object_counts_rd ON metrics.workspace_object_counts (record_date);

CREATE INDEX idx_workspace_object_counts_toc ON metrics.workspace_object_counts (total_object_count);

CREATE INDEX idx_workspace_object_counts_tloc ON metrics.workspace_object_counts (top_lvl_object_count);

CREATE INDEX idx_workspace_object_counts_puoc ON metrics.workspace_object_counts (public_object_count);

CREATE INDEX idx_workspace_object_counts_proc ON metrics.workspace_object_counts (private_object_count);



############################################
# users_workspace_object_types

CREATE OR REPLACE TABLE metrics.users_workspace_object_counts (
    object_type VARCHAR(255) NOT NULL,
    object_spec_version VARCHAR(255) NOT NULL,
    object_type_full VARCHAR(255) NOT NULL,
    record_date DATE NOT NULL,
    last_mod_date DATE NOT NULL,
    top_lvl_object_count INTEGER NOT NULL,
    total_object_count INTEGER NOT NULL,
    public_object_count INTEGER NOT NULL,
    private_object_count INTEGER NOT NULL,
    hidden_object_count INTEGER NOT NULL DEFAULT 0,
    deleted_object_count INTEGER NOT NULL DEFAULT 0,
    copy_count INTEGER NOT NULL,
    total_size BIGINT NOT NULL,
    top_lvl_size BIGINT NOT NULL,
    max_object_size BIGINT NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE UNIQUE INDEX uk_type_ver_rd_uworkspace_object_counts
ON metrics.users_workspace_object_counts(object_type, object_spec_version, record_date);

CREATE INDEX idx_uworkspace_object_counts_ot ON metrics.users_workspace_object_counts (object_type);

CREATE INDEX idx_uworkspace_object_counts_osv ON metrics.users_workspace_object_counts (object_spec_version);

CREATE INDEX idx_uworkspace_object_counts_otf ON metrics.users_workspace_object_counts (object_type_full);

CREATE INDEX idx_uworkspace_object_counts_rd ON metrics.users_workspace_object_counts (record_date);

CREATE INDEX idx_uworkspace_object_counts_toc ON metrics.users_workspace_object_counts (total_object_count);

CREATE INDEX idx_uworkspace_object_counts_tloc ON metrics.users_workspace_object_counts (top_lvl_object_count);

CREATE INDEX idx_uworkspace_object_counts_puoc ON metrics.users_workspace_object_counts (public_object_count);

CREATE INDEX idx_uworkspace_object_counts_proc ON metrics.users_workspace_object_counts (private_object_count);


##################################################
# Suspect shock nodes (initially for narratives, may have other objects down the line)

CREATE OR REPLACE TABLE `suspect_shock_nodes` (
  `ws_obj_reference` varchar(255) NOT NULL,
  `shock_node` varchar(255) NOT NULL,
  `first_seen_date` date NOT NULL,
  `last_seen_date` date NOT NULL,
  `ws_id` int(11) NOT NULL,  
  UNIQUE KEY `uk_wsref_node_ssn` (`ws_obj_reference`,`shock_node`),
  KEY `idx_ssn_fsd` (`first_seen_date`),
  KEY `idx_ssn_lsd` (`last_seen_date`),
  KEY `idx_ssn_wsid` (`ws_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

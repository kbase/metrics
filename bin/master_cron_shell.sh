#!/bin/bash

python daily_cron_jobs/upload_user_stats.py

python daily_cron_jobs/upload_elasticsearch_usersmry_stats.py

python daily_cron_jobs/upload_app_stats.py

python daily_cron_jobs/upload_file_stats.py

python daily_cron_jobs/upload_app_category_mappings.py

python daily_cron_jobs/upload_public_narratives_count.py

python daily_cron_jobs/upload_user_orcid_count.py

python daily_cron_jobs/make_reporting_tables.py



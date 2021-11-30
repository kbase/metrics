#!/bin/bash

python monthly_cron_jobs/methods_upload_publication_metrics.py
python daily_cron_jobs/upload_user_stats.py
python monthly_cron_jobs/upload_workspace_stats.py

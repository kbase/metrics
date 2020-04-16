#!/bin/bash

python daily_cron_jobs/upload_user_stats.py
python monthly_cron_jobs/upload_workspace_stats.py

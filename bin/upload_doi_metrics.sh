#!/bin/bash
python monthly_cron_jobs/populate_downloading_apps.py
python daily_cron_jobs/upload_user_stats.py
python monthly_cron_jobs/methods_upload_doi_metrics.py

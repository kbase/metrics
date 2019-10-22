#!/bin/bash

python upload_user_stats.py

python upload_elasticsearch_usersmry_stats.py

python upload_app_stats.py

python upload_app_category_mappings.py

python upload_public_narratives_count.py

python make_reporting_tables.py



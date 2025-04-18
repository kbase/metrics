# UploadAppStats
#
import methods_upload_app_stats_old_app_catalog
import time
import datetime

print("############################################")
print("App Stats Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
# start_date = "2012-02-01"
start_date = "2020-03-01"
end_date = "2020-10-30"
methods_upload_app_stats_old_app_catalog.upload_user_app_stats(start_date, end_date)
# methods_upload_app_stats_old_app_catalog.upload_user_app_stats()
print("Finished): " + str(datetime.datetime.utcnow()))
print("Uploading app stats took ", time.time() - start_time, " seconds to run")
print("############################################")
# UploadAppStats
#
import methods_upload_app_stats
import time
import datetime

print("############################################")
print("App Stats Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
#start_date = "2023-07-27"
#end_date = "2023-08-01"
#methods_upload_app_stats.upload_user_app_stats(start_date, end_date)
methods_upload_app_stats.upload_user_app_stats()
print("Uploading app stats took ", time.time() - start_time, " seconds to run")

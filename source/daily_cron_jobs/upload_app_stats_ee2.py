# UploadAppStats
#
import methods_upload_app_stats_ee2
import time
import datetime
print("############################################")
print("App Stats Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
start_date = "2012-02-01"
#start_date = "2019-08-15"
end_date = "2020-04-20"
methods_upload_app_stats_ee2.upload_user_app_stats(start_date,end_date)
#methods_upload_app_stats.upload_user_app_stats()
print("Uploading app stats took ", time.time() - start_time, " seconds to run")


# UploadAppStats
#
import methods_upload_app_stats_ee2_cpu
import time
import datetime

print("############################################")
print("App Stats Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
#start_date = "2002-02-01"
#end_date = "2020-06-20"
#methods_upload_app_stats_ee2_cpu.upload_user_app_stats(start_date, end_date)
methods_upload_app_stats_ee2_cpu.upload_user_app_stats()
print("Uploading app stats took ", time.time() - start_time, " seconds to run")

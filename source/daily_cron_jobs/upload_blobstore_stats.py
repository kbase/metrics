# UploadAppStats
#
import methods_upload_blobstore_stats
import time
import datetime

print("############################################")
print("Blobstore Stats Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
#start_date = "2021-09-10"
#end_date = "2021-09-15"
#methods_upload_blobstore_stats.process_blobstore_stats_data(start_date,end_date)
methods_upload_blobstore_stats.process_blobstore_stats_data()
print("Uploading blobstore stats took ", time.time() - start_time, " seconds to run")

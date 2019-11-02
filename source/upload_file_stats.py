# UploadAppStats
#
import methods_upload_file_stats
import time
import datetime
print("############################################")
print("FILE Stats Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
#start_date = "1000-01-01"
#end_date = "2019-11-02"
#methods_upload_file_stats.process_file_stats_data(start_date,end_date)
methods_upload_file_stats.process_file_stats_data()
print("Uploading file stats took ", time.time() - start_time, " seconds to run")











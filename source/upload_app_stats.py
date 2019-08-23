# UploadAppStats
#

import methods_upload_app_stats
import time
start_time = time.time()
#start_date = "2019-02-01"
#end_date = "2019-02-02"
#upload_user_app_stats(start_date,end_date)
methods_upload_app_stats.upload_user_app_stats()
print("Uploading app stats took ", time.time() - start_time, " seconds to run")


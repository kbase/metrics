import methods_upload_public_narratives_count
import time
import datetime

print("############################################")
print("Public Narratives count Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
methods_upload_public_narratives_count.upload_public_narratives_count()
print("Finished: " + str(datetime.datetime.utcnow()))
print("--- public narratives count time :  %s seconds ---" % (time.time() - start_time))
print("############################################")
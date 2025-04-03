# UploadBlobstoreDetails
#
import methods_upload_blobstore_details
import time
import datetime

yesterday = datetime.date.today() - datetime.timedelta(days=1)
print("############################################")
print("############################################")
print("############################################")
print("Blobstore Details Upload (UTC): " + str(datetime.datetime.utcnow()))
print("START TIME (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()


start_time = time.time()
#start_date = "2024-09-07"
#end_date = "2024-10-28"
#methods_upload_blobstore_details.process_blobstore_details_data(start_date,end_date)
methods_upload_blobstore_details.process_blobstore_details_data()
print("Uploading blobstore details took ", time.time() - start_time, " seconds to run")


start_date=datetime.datetime.combine(yesterday, datetime.datetime.min.time())
end_date=datetime.datetime.combine(yesterday, datetime.datetime.max.time())

print("Start date: " + str(start_date))
print("End date: " + str(end_date))
print("############################################")
import methods_upload_app_category_mappings
import time
import datetime

print("############################################")
print("App Category Mapping Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
methods_upload_app_category_mappings.update_app_category_mappings()
print("Finished: " + str(datetime.datetime.utcnow()))
print("--- app_cat_mapping time :  %s seconds ---" % (time.time() - start_time))
print("############################################")
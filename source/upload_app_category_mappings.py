import methods_upload_app_category_mappings
import time
import datetime
print("App Category Mapping Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
methods_upload_app_category_mappings.update_app_category_mappings()
print("--- app_cat_mapping time :  %s seconds ---" % (time.time() - start_time))

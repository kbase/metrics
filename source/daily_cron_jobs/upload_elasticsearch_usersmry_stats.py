# UploadElasticsearchStats
import warnings
import pprint

warnings.simplefilter(action="ignore", category=Warning)
import methods_upload_elasticsearch_sumrydicts
import time
import datetime


print("############################################")
print("Elastic Search Session Info Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
# start_date = "month-day-year"
# end_date = "month-day-year"
#start_date = "07-27-2023"
#end_date = "08-01-2021"
#return_capture = methods_upload_elasticsearch_sumrydicts.elastic_summary_dictionaries(
#    start_date, end_date
#)
return_capture = methods_upload_elasticsearch_sumrydicts.elastic_summary_dictionaries()

print("--- gather data %s seconds ---" % (time.time() - start_time))

# pprint.pprint(return_capture)
print("NUMBER OF USER SESSIONS RETRIEVED : " + str(len(return_capture)))
methods_upload_elasticsearch_sumrydicts.upload_elastic_search_session_info(
    return_capture
)

print("--- including gather and upload %s seconds ---" % (time.time() - start_time))

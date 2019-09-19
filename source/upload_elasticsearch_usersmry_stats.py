
# UploadElasticsearchStats                                                                                                                                   
import warnings
import pprint
warnings.simplefilter(action='ignore', category=Warning)            
import methods_upload_elasticsearch_sumrydicts
# start_date = "month-day-year"                                                                                                                              
# end_date = "month-day-year"  
return_capture = methods_upload_elasticsearch_sumrydicts.elastic_summary_dictionaries()
pprint.pprint(return_capture)

# GetAppStats
#
import requests
import os

from datetime import date, timedelta, datetime
from biokbase.narrative_method_store.client import NarrativeMethodStore
from installed_clients.execution_engine2Client import execution_engine2

requests.packages.urllib3.disable_warnings()

ee2_url = os.environ["EE2_URL"]
# GetEE2AppStats
ee2 = execution_engine2(
    url=ee2_url,
    token=os.environ["METRICS_USER_TOKEN"],
)

nms = NarrativeMethodStore(url=os.environ["NARRATIVE_METHOD_STORE"])
sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]

# Insures all finish times within last day.
#yesterday = datetime.date.today() - datetime.timedelta(days=1)

#get first day of the month:
#first_of_this_month = datetime.today().replace(day=1)
date_today = datetime.now()
first_of_this_month = date_today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
print(str(first_of_this_month))
last_day_of_prev_month = first_of_this_month.replace(day=1) - timedelta(days=1)
first_of_previous_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)

#first_of_previous_month = datetime(first_of_previous_month).replace(hour=0, minute=0, second=0, microsecond=0)
print(str(first_of_this_month))
print(str(first_of_previous_month))
end = int(first_of_this_month.strftime("%s")) * 1000
begin = int(first_of_previous_month.strftime("%s")) * 1000
print("End :" + str(end))
print("Begin :" + str(begin))

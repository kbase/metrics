from pymongo import MongoClient
import os
from arango import ArangoClient

mongoDB_metrics_connection = os.environ["MONGO_PATH"]
to_workspace = os.environ["WRK_SUFFIX"]

client = MongoClient(mongoDB_metrics_connection + to_workspace)
db = client.workspace

re_host_url = os.environ["RE_HOST_URL"]
re_username = os.environ["RE_USERNAME"]
re_pwd = os.environ["RE_PWD"]

re_client = ArangoClient(hosts=re_host_url)
re_db = re_client.db('prod', username=re_username, password=re_pwd)

def get_ws_prov_descendant_of_count():
    cursor = re_db.aql.execute("FOR doc IN ws_prov_descendant_of COLLECT WITH COUNT INTO length RETURN length")
    results =  [doc for doc in cursor]
    number_of_ws_prov_descendant_of_records = 0
    print("ARANGO RESULTS ws_prov_descendant_of size: " + str(results[0]))
    return 1
    
def get_ws_obj_versions_provref_count(db):
    total_obj_with_prov_refs = 0
    total_prov_refs = 0
    ws_obj_vers_cursor = db.workspaceObjVersions.find({"provrefs":{ "$exists": True, "$ne": []}},{"id":1,"ver":1,"provrefs":1,"_id":0})
    for ws_obj_ver in ws_obj_vers_cursor:
        total_obj_with_prov_refs += 1
        total_prov_refs +=  len(ws_obj_ver["provrefs"])
    print("total_obj_with_prov_refs : " + str(total_obj_with_prov_refs))
    print("total_prov_refs : " + str(total_prov_refs))
    return 1

get_ws_prov_descendant_of_count()
get_ws_obj_versions_provref_count(db)
print("END")

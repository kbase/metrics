from pymongo import MongoClient
import os

mongoDB_metrics_connection = os.environ["MONGO_PATH"]
to_workspace = os.environ["WRK_SUFFIX"]

client = MongoClient(mongoDB_metrics_connection + to_workspace)
db = client.workspace

#ws_ids = [112263]
ws_ids = [33233]

def build_internally_derived_objects_mapp(db, ws_id):
    #Build derived objects where both the input and output objects are from the DOI WS
    levels_deep = 0
    objects_used_as_inputs = set()
    derived_objects_lookups = dict()   #input object id -> set(output_object_ids)  
    ws_obj_vers_cursor = db.workspaceObjVersions.find({"ws":ws_id,"provrefs":{ "$exists": True, "$ne": []}, "type":{"$ne":"KBaseReport.Report-2.0"}},{"id":1,"ver":1,"provrefs":1,"_id":0})
    for ws_obj_ver in ws_obj_vers_cursor:
        provref_inputs =  ws_obj_ver["provrefs"]
        output_ref = str( ws_id) + "/" + str(ws_obj_ver["id"]) + "/" + str(ws_obj_ver["ver"])
        for provref_input in provref_inputs:
            objects_used_as_inputs.add(provref_input)
            if provref_input not in derived_objects_lookups:
                derived_objects_lookups[provref_input] = set()
            derived_objects_lookups[provref_input].add(output_ref)
    print("derived_objects_lookups: " + str(derived_objects_lookups) + " Length of :" + str(len(derived_objects_lookups)))
    print("objects_used_as_inputs: " + str(objects_used_as_inputs) + " Length of :" + str(len(objects_used_as_inputs)))

    
for ws_id in ws_ids:
    build_internally_derived_objects_mapp(db, ws_id)

from pymongo import MongoClient
from pymongo import ReadPreference
from biokbase.workspace.client import Workspace
import json as _json
import os
from datetime import datetime

"""
THIS SCRIPT WAS CREATED TO GET A FEEL FOR HOW WIDE SPREAD OF AN ISSUE MULTIPLE NARRATIVE WORKSPACES.
THESE ARE WORKSPACES THAT HAVE MORE THAN ONE NARRATIVE OBJECT WITH DIFFERENT OBJECT IDS.
DIFFERENT VERSIONA ALONE ARE FINE. 
BASED ON THIS I HAD TO CHANGE LOGIC IN THE MONTHLY WORKSPACE STATS JOB
"""

mongoDB_metrics_connection = os.environ["MONGO_PATH"]
to_workspace = os.environ["WRK_SUFFIX"]

# MONGO_PWD=crinkle-friable-smote
# WRK_SUFFIX=workspace?readPreference=secondary

# mongoDB_metrics_connection="crinkle-friable-smote"
# to_workspace="workspace?readPreference=secondary"

client = MongoClient(mongoDB_metrics_connection + to_workspace)
db = client.workspace

ws_narratives_dict = dict()
# {"ws":{"$gt":56000},
ws_obj_vers_cursor = db.workspaceObjVersions.find(
    {
        "type": {
            "$in": [
                "KBaseNarrative.Narrative-1.0",
                "KBaseNarrative.Narrative-2.0",
                "KBaseNarrative.Narrative-3.0",
                "KBaseNarrative.Narrative-4.0",
            ]
        }
    },
    {"ws": 1, "id": 1, "ver": 1, "savedate": 1, "_id": 0},
)
for ws_obj_ver in ws_obj_vers_cursor:
    #    print(str(ws_obj_ver))
    if ws_obj_ver["ws"] not in ws_narratives_dict:
        ws_narratives_dict[ws_obj_ver["ws"]] = dict()
    if ws_obj_ver["id"] not in ws_narratives_dict[ws_obj_ver["ws"]]:
        ws_narratives_dict[ws_obj_ver["ws"]][ws_obj_ver["id"]] = {
            "max_version": 1,
            "savedate": None,
        }
    if (
        ws_obj_ver["ver"]
        > ws_narratives_dict[ws_obj_ver["ws"]][ws_obj_ver["id"]]["max_version"]
    ):
        ws_narratives_dict[ws_obj_ver["ws"]][ws_obj_ver["id"]][
            "max_version"
        ] = ws_obj_ver["ver"]

multiple_narratives_ws_count = 0
for ws in sorted(ws_narratives_dict.keys()):
    if len(ws_narratives_dict[ws]) > 1:
        print(
            "WS "
            + str(ws)
            + " has "
            + str(len(ws_narratives_dict[ws]))
            + " different narratives"
        )
        multiple_narratives_ws_count += 1
        print("Workspace: " + str(ws) + " : " + str(ws_narratives_dict[ws]))
print(
    "TOTAL NUMBER OF WORKSPACES WITH MULTIPLE NARRATIVES "
    + str(multiple_narratives_ws_count)
)

#Declare Functions
#!/usr/bin/python3

from pymongo import MongoClient
from pymongo import ReadPreference
import json as _json

# NOTE get_user_info_from_auth2 sets up the initial dict. The following functions update certain fields in the dict.
# So get_user_info_from_auth2 must be called before get_internal_users and get_user_orgs_count

metrics_password = "NEED TO CHANGE"

def get_user_info_from_auth2():
    #get auth2 info and kbase_internal_users. Creates initial dict for the data.
    client_auth2 = MongoClient("mongodb://kbasemetrics:"+metrics_password+"@db5.chicago.kbase.us/auth2?readPreference=secondary")

    db_auth2 = client_auth2.auth2
    
    user_stats_dict = {} #dict that will have userid as the key,
                         #value is a dict with name, signup_date, last_signin_date, and email (that gets values from this function)
                         #orcid may be present and populated by this function.
                         #later called functions will populate kbase_internal_user, num_orgs and ...

    user_info_query = db_auth2.users.find({},{"_id":0,"user":1,"email":1,"display":1,"create":1,"login":1})
    for record in user_info_query:
        user_stats_dict[record["user"]]={"name":record["display"],
                                         "signup_date":record["create"],
                                         "last_signin_date":record["login"],
                                         "email":record["email"],
                                         "kbase_internal_user":False,
                                         "institution":None,
                                         "country":None,
                                         "num_orgs":0,
                                         "narrative_count":0,
                                         "shared_count":0,
                                         "narratives_shared" : 0                                        
                                         }

    #Get all users with an ORCID authentication set up.
    users_orcid_query = db_auth2.users.find({"idents.prov": "OrcID"},{"user":1,"idents.prov":1,"idents.prov_id":1,"_id":0})
    for record in users_orcid_query:
        for ident in record["idents"]:
            if ident["prov"] == "OrcID":
                #just use the first orcid seen.
                user_stats_dict[record["user"]]["orcid"] = ident["prov_id"]
                continue
                
    client_auth2.close()
    return user_stats_dict

def get_internal_users(user_stats_dict):
    client_metrics = MongoClient("mongodb://kbasemetrics:"+metrics_password+"@db5.chicago.kbase.us/metrics?readPreference=secondary")
    db_metrics = client_metrics.metrics
    kb_internal_user_query = db_metrics.users.find({"kbase_staff":True},{"_id":0,"username":1,"kbase_staff":1})
    for record in kb_internal_user_query:
        if record["username"] in user_stats_dict:
            user_stats_dict[record["username"]]["kbase_internal_user"] = True
    client_metrics.close()  
    return user_stats_dict
            
def get_user_orgs_count(user_stats_dict):
    client_orgs = MongoClient("mongodb://kbasemetrics:"+metrics_password+"@db5.chicago.kbase.us/groups?readPreference=secondary")
    db_orgs = client_orgs.groups
    orgs_query = db_orgs.groups.find({},{"name":1,"memb.user":1,"_id":0})
    for record in orgs_query:
        for memb in record["memb"]:
            if memb["user"] in user_stats_dict:
                user_stats_dict[memb["user"]]["num_orgs"] += 1
    client_orgs.close()  
    return user_stats_dict  

def get_user_narrative_stats(user_stats_dict):  
    client_workspace = MongoClient("mongodb://kbasemetrics:"+metrics_password+"@db5.chicago.kbase.us/workspace?readPreference=secondary")
    db_workspace = client_workspace.workspace
    ws_user_dict = {}
    #Get all the legitimate narratives and and their respective user (not del, saved(not_temp))
    all_nar_cursor = db_workspace.workspaces.find({"del" : False,
                                                   "meta" : {"k" : "is_temporary", "v" : "false"} },
                                                  {"owner":1,"ws":1,"name":1,"_id":0})
    for record in all_nar_cursor:
        # TO REMOVE OLD WORKSPACE METHOD OF 1 WS for all narratives.
        if "name" in record and record["name"] == record["owner"] + ":home" :
            continue
        #narrative to user mapping
        ws_user_dict[record["ws"]] = record["owner"]
        #increment user narrative count
        user_stats_dict[record["owner"]]["narrative_count"] += 1

    #Get all the narratives that have been shared and how many times they have been shared.
    aggregation_string=[{
        "$match" : {"perm" : { "$in": [ 10,20,30 ]}}
        },{
            "$group" : {"_id" : "$id", "shared_count" : { "$sum" : 1 }}
        }]
    all_shared_perms_cursor=db_workspace.workspaceACLs.aggregate(aggregation_string)

    for record in db_workspace.workspaceACLs.aggregate(aggregation_string):
        if record["_id"] in ws_user_dict:
            user_stats_dict[ws_user_dict[record["_id"]]]["shared_count"] += record["shared_count"]
            user_stats_dict[ws_user_dict[record["_id"]]]["narratives_shared"] += 1

    return user_stats_dict

class _JSONObjectEncoder(_json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, frozenset):
            return list(obj)
        return _json.JSONEncoder.default(self, obj)

def get_institution_and_country(user_stats_dict):  
    url = "https://kbase.us/services/user_profile/rpc"
    headers = dict()
    arg_hash = {'method': "UserProfile.get_user_profile",
            'params': [user_stats_dict.keys()],
            'version': '1.1',
            'id': 123
            }
    body = _json.dumps(arg_hash, cls=_JSONObjectEncoder)
    timeout = 1800
    trust_all_ssl_certificates = 1

    ret = requests.post(url, data=body, headers=headers,
                             timeout=timeout,
                             verify=not trust_all_ssl_certificates)
    ret.encoding = 'utf-8'
    if ret.status_code == 500:
        if ret.headers.get(_CT) == _AJ:
            err = ret.json()
            if 'error' in err:
                raise Exception(err)
            else:
                raise ServerError('Unknown', 0, ret.text)
        else:
            raise ServerError('Unknown', 0, ret.text)
    if not ret.ok:
        ret.raise_for_status()
    resp = ret.json()
    if 'result' not in resp:
        raise ServerError('Unknown', 0, 'An unknown server error occurred')
    print len(resp['result'][0])
    replaceDict = { '-':' ', ')':' ', '.': ' ', '(':'', '/':'', ',':'', ' +': ' ' }
    counter = 0
    for obj in resp['result'][0] :
        if obj is None:
            continue  
        counter += 1;

        if obj['user']['username'] in user_stats_dict:
            if obj['user']['username'] == 'jkbaumohl':
                print(str(obj))
                print("JKBAUMOHL COUNTRY: " + str(obj['profile']['userdata'].get('country')))
            user_stats_dict[obj['user']['username']]["country"] = obj['profile']['userdata'].get('country')
            institution = obj['profile']['userdata'].get('organization')
            if institution == None:
                if 'affiliations'in obj['profile']['userdata']:
                    affiliations = obj['profile']['userdata']['affiliations']
                    try:
                        institution = affiliations[0]['organization']
                    except IndexError:
                        try:
                            institution = obj['profile']['userdata']['organization']
                        except:
                            pass
            if institution:
                for key, replacement in replaceDict.items():
                    #institution = institution.str.replace(key, replacement)
                    institution = institution.replace(key, replacement)
                institution = institution.rstrip()
            user_stats_dict[obj['user']['username']]["institution"] = institution
    return user_stats_dict

import time
start_time = time.time()
user_stats_dict = get_user_info_from_auth2()
user_stats_dict = get_internal_users(user_stats_dict)
user_stats_dict = get_user_orgs_count(user_stats_dict)
user_stats_dict = get_user_narrative_stats(user_stats_dict)
user_stats_dict = get_institution_and_country(user_stats_dict)
print(str(user_stats_dict[u'jkbaumohl']))
print(str(user_stats_dict[u'zcrockett']))
print(str(user_stats_dict[u'gonzalonm']))
#print(str(user_stats_dict))
print("--- %s seconds ---" % (time.time() - start_time))

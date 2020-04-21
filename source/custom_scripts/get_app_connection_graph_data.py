from biokbase.workspace.client import Workspace
#from pymongo import MongoClient
#from pymongo import ReadPreference
import os
import mysql.connector as mysql

"""
THIS NEEDS TO BE RUN ON MYSQL1 as it requires the special token to investigate the workspaces

THIS CREATES A TXT OUPUT THAT LOOKS LIKE THIS THAT ADAM THEN INGESTS INTO R TO DO HIS SPECIAL NETWORK GRAPH

Narrative ID	Owner	Creation Date	Last Modified	is_deleted	is_public	App_Name_1	App_Categories_1	App_Count_1	App_Name_2	App_Categories_2	App_Count_2 ... ... App_Name_50	App_Categories_50	App_Count_50
49286	a0912	2019-10-05	2019-10-05	0	0	
39835	a20080600	2019-01-25	2019-02-21	0	0	fba_tools/bulk_download_modeling_objects	metabolic_modeling	1	fba_tools/compare_fba_solutions	metabolic_modeling	1	fba_tools/compare_flux_with_expression	expression;metabolic_modeling	1	fba_tools/compare_models	metabolic_modeling	1	kb_uploadmethods/import_file_as_fba_model_from_staging		3	kb_uploadmethods/import_genbank_as_genome_from_staging		1
40766	a20080600	2019-02-21	2019-02-21	0	0	

14906	a7med	2016-05-16	2016-05-16	0	0	annotate_contigset		1	assemble_contigset_from_reads		1
15596	a9887688zboy	2016-06-17	2016-06-17	0	0	fba_tools/build_metabolic_model	metabolic_modeling	1
12166	aaaring	2016-01-08	2016-01-08	0	0	
24863	aafoutouhi	2017-09-21	2017-09-22	0	0	ProkkaAnnotation/annotate_contigs	annotation	3	RAST_SDK/reannotate_microbial_genome	annotation	2	RAST_SDK/reannotate_microbial_genomes	annotation	1	kb_SPAdes/run_SPAdes	assembly	2	kb_blast/BLASTn_Search	sequence	1	kb_trimmomatic/run_trimmomatic	reads	2	kb_uploadmethods/import_fastq_sra_as_reads_from_staging		1
"""

metrics_mysql_password = os.environ['METRICS_MYSQL_PWD']
#mongoDB_metrics_connection = os.environ['MONGO_PATH']

sql_host = os.environ['SQL_HOST']
query_on = os.environ['QUERY_ON']
#to_workspace =  os.environ['WRK_SUFFIX']

ws_url = os.environ['WS_URL']
ws_user_token = os.environ["METRICS_WS_USER_TOKEN"]

def get_workspaces(db_connection):
    """
    gets user narrative workspaces - capturing ws_id, owner, creation_date, last_mod_date, is_deleted, is_public. 
    """
    workspaces_dict = {}

    cursor = db_connection.cursor()
    query = "use "+query_on
    cursor.execute(query)

    query = "select ws_id, ws.username as username, initial_save_date, mod_date, "\
            "is_deleted, is_public "\
            "from metrics_reporting.workspaces_current ws "\
            "inner join metrics.user_info ui on ws.username = ui.username "\
            "where ws.narrative_version > 0  "\
            "and ui.kb_internal_user = 0;"

    cursor.execute(query)
    for (record) in cursor:
        workspaces_dict[record[0]] = {"username" : record[1],
                                      "creation_date" : record[2],
                                      "mod_date" : record[3],
                                      "is_deleted" : record[4],
                                      "is_public" : record[5],
                                      "apps_list" : list()}

    # App category mappings
    app_category_lookup = dict() #key app name, value concatenated categories ";" separated
    query = "select app_name, app_category from app_name_category_map order by app_name, app_category;"
    cursor.execute(query)
    previous_app_name = ""
    for (record) in cursor:
        if previous_app_name != record[0]:
            previous_app_name = record[0]    
            app_category_lookup[record[0]] = record[1]
        else:
            app_category_lookup[record[0]] = app_category_lookup[record[0]] + ";" + record[1]
    print
    return (workspaces_dict,app_category_lookup)

def get_app_connection_data(workspaces_dict, app_category_lookup):
    """
    Gets the apps that have been run and their count in the narrative, builds up the app_list.
    See documentation for WS administer here
    https://github.com/kbase/workspace_deluxe/blob/02217e4d63da8442d9eed6611aaa790f173de58e/docsource/administrationinterface.rst
    """
    wsadmin = Workspace(ws_url, token=ws_user_token)
    max_app_count = 0
    for ws_id in workspaces_dict:
        if workspaces_dict[ws_id]["is_deleted"] == 1:
            continue
            #deleted workspaces do not have these objects to look at
            #because this is first of the month set field can change.
        try:
            ws_info = wsadmin.administer({'command': "getWorkspaceInfo",
                                          'params':  {"id": str(ws_id)}})
            ws_info_dict = ws_info[8]
            narr_obj_id = None
            if "narrative" in ws_info_dict:
                narr_obj_id = ws_info_dict["narrative"]
                narrative_ref = str(ws_id) + "/" + str(narr_obj_id)
                info = wsadmin.administer({'command': "getObjectInfo",
                                           'params':  {"objects": [{"ref": narrative_ref}], "includeMetadata": 1}
                })["infos"][0]
                meta = info[10]
                app_dict = dict() #keeps track of dicts so we can add apps together with differeny git commit hashes
                for key in meta:
                    #NEEDS THE period after method. There is anothe key called "method"
                    if key.startswith("method."):
                        app_name = key[7:]
                        app_name = app_name.rsplit("/",1)[0]
                        if app_name not in app_dict:
                            app_dict[app_name] = 0
                        app_dict[app_name] +=  int(meta[key])
                if len(app_dict) > max_app_count:
                    max_app_count = len(app_dict)
                for app_name in sorted(app_dict):
                    app_categories = ""
                    if app_name in app_category_lookup:
                        app_categories = app_category_lookup[app_name]
                    workspaces_dict[ws_id]["apps_list"].append(app_name)
                    workspaces_dict[ws_id]["apps_list"].append(app_categories)
                    workspaces_dict[ws_id]["apps_list"].append(str(app_dict[app_name]))
        except:
            # means the workspace was likely deleted since the monthly run.
            workspaces_dict[ws_id]["is_deleted"] = 1
            continue
                
    return(workspaces_dict,max_app_count)
                    

def get_app_connection_graph_data():
    """
    get the apps that ran on the WS
    """

    #connect to mysql
    db_connection = mysql.connect(
        host = sql_host,
        user = "metrics",
        passwd = metrics_mysql_password,
        database = "metrics"
    )

    cursor = db_connection.cursor()
    query = "use "+query_on
    cursor.execute(query)
    
    (workspaces_dict,app_category_lookup) = get_workspaces(db_connection)
#    temp_dict = dict()
#    temp_dict[49114] = workspaces_dict[49114]
#    temp_dict[51672] = workspaces_dict[51672]
#    workspaces_dict.clear()
#    workspaces_dict = temp_dict
    
    (workspaces_dict,max_app_count) = get_app_connection_data(workspaces_dict,app_category_lookup)

    ################
    # Print the header line:
    ################
    header_line = "Narrative ID\tOwner\tCreation Date\tLast Modified\tis_deleted\tis_public"
    for i in range(max_app_count):
        header_line += "\tApp_Name_{}\tApp_Categories_{}\tApp_Count_{}".format(str(i+1),str(i+1),str(i+1))
    print(header_line)

    ###############
    # Print the WS rows
    ###############
    for ws_id in workspaces_dict:
        print("{}\t{}\t{}\t{}\t{}\t{}\t{}".format(str(ws_id),
                                                  workspaces_dict[ws_id]["username"],
                                                  workspaces_dict[ws_id]["creation_date"],
                                                  workspaces_dict[ws_id]["mod_date"],
                                                  str(workspaces_dict[ws_id]["is_deleted"]),
                                                  str(workspaces_dict[ws_id]["is_public"]),
                                                  "\t".join(workspaces_dict[ws_id]["apps_list"])))
    
get_app_connection_graph_data()

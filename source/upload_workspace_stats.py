import time
import methods_upload_workspace_stats
import datetime
print("############################################")
print("Workspace and Object Stats Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()
methods_upload_workspace_stats.upload_workspace_stats()
print("--- Total TIME %s seconds ---" % (time.time() - start_time))




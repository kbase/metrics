import time
import methods_upload_user_stats

start_time = time.time()
user_stats_dict = methods_upload_user_stats.get_user_info_from_auth2()
user_stats_dict = methods_upload_user_stats.get_internal_users(user_stats_dict)
user_stats_dict = methods_upload_user_stats.get_user_orgs_count(user_stats_dict)
user_stats_dict = methods_upload_user_stats.get_user_narrative_stats(user_stats_dict)
user_stats_dict = methods_upload_user_stats.get_institution_and_country(user_stats_dict)
print("--- gather data %s seconds ---" % (time.time() - start_time))
methods_upload_user_stats.upload_user_data(user_stats_dict)
print("--- including user info and user_stats upload %s seconds ---" % (time.time() - start_time))






from methods_elasticquery import retrieve_elastic_response
import warnings
import time
warnings.simplefilter(action='ignore', category=Warning)
import pandas as pd
import datetime
yesterday = (datetime.date.today() - datetime.timedelta(days=1))


# Json dictionaries to data dictionaries
def results_to_formatted_dicts(query_results):
    """results_to_formatted_dicts takes raw elasticsearch json dictionaries and
    flattens to basic user-narrative dictionary."""

    # Initialize array and get data/'hits'
    data_formatted = []
    data = [doc for doc in query_results['hits']['hits']]

    entries_1 = ('type', 'instance', '@version', 'index', 'geoip')
    entries_2 = ('highlight', 'fields', 'location', '_score', '_index', '_source', '_type', 'sort')

    for doc in data:
        source_dictionary = doc['_source']
        # Check that geoip information is valid and uncorrupted
        if 'geoip'in source_dictionary:
            # Check if geoip key is present but empty
            if not source_dictionary['geoip']:
                for key in entries_1:
                    if key in source_dictionary:
                        del source_dictionary[key]
                # Collect epoch timestamp and update dictionary with geoip items
                epoch_timestamp = doc['fields']['@timestamp'][0]
                doc.update(source_dictionary)
            
                # Delete second level keys for final flattened dictionary
                for key in entries_2:
                    if key in doc:
                        del doc[key]
                # Add epoch timestamp and collect ip error tag
                doc['epoch_timestamp'] = epoch_timestamp
                doc['tags'] = doc['tags'][0]
                data_formatted.append(doc)
            
            else:
               # Delete duplicate country code and rename country_code2 -> country_code
               del source_dictionary['geoip']["country_code3"]
               source_dictionary['geoip']["country_code"] = source_dictionary['geoip'].pop("country_code2")
               # Collect all items in geoip dictionary
               geoip_items = source_dictionary['geoip']

               # Delete first level keys entries
               for key in entries_1:
                   if key in source_dictionary:
                       del source_dictionary[key]
                    
               # Collect epoch timestamp and update dictionary with geoip items
               epoch_timestamp = doc['fields']['@timestamp'][0]
               doc.update(geoip_items)
               doc.update(source_dictionary)

               # Delete second level keys for final flattened dictionary
               for key in entries_2:
                   if key in doc:
                       del doc[key]
                    # Add epoch timestamp and append dictionary
               doc['epoch_timestamp'] = epoch_timestamp
               data_formatted.append(doc)
        else:
            continue

    return data_formatted


def elasticsearch_pull(start_date, end_date):
    """ Elasticsearch_pull takes a string value, or default datetime, for start_date and end_date and generates an elasticsearch query that
    pulls user-narrative information for that date range and formats the data to flattened user dictionaries."""
    #start_time = time.time()
    if type(start_date) == str:
        # Format date strings to datetime objects
        start_date = datetime.datetime.strptime(start_date, '%m-%d-%Y')
        start_date = datetime.datetime.combine(start_date, datetime.datetime.min.time())
        end_date = datetime.datetime.strptime(end_date, '%m-%d-%Y')
        end_date = datetime.datetime.combine(end_date, datetime.datetime.max.time())

        # datetime to epoch. Epoch format needed for elastic query
        epoch_start = int(start_date.strftime('%s')) * 1000
        epoch_end = int(end_date.strftime('%s')) * 1000
    else:
        # datetime to epoch. Epoch format needed for elastic query
        epoch_start = int(start_date.strftime('%s')) * 1000
        epoch_end = int(end_date.strftime('%s')) * 1000

    # Return results of elastic query and format data to dictionary structures
    results = retrieve_elastic_response(epoch_start, epoch_end)
    data_array = results_to_formatted_dicts(results)

    # Get relative sizes of data
    total_results = results['hits']['total']
    size_results_pulled = len(results['hits']['hits'])

    # Start array from older timestamp for an overlap of 10 values
    new_start = data_array[-10]
    timestamp = [new_start['epoch_timestamp']]

    while size_results_pulled < total_results:
        results_sequential = retrieve_elastic_response(epoch_start, epoch_end, timestamp)
        data_additional = results_to_formatted_dicts(results_sequential)

        # Increment data counts
        size_results_pulled += len(results_sequential['hits']['hits'])
        # Start array from older timestamp for an overlap of 10 values
        try:
            new_start = data_additional[-10]
            timestamp = [new_start['epoch_timestamp']]
        except:
            pass
        data_array.extend(data_additional)

    #print("Elasticsearch data took from {}-{} took {} seconds to retrieve".format(start_date, end_date, time.time() - start_time))
    return data_array


def make_user_activity_dict(data, ip, user):
    """make_user_activity_dict makes a summary dictionary for a given user based on their elasticsearch data
     and narrative usage. """
    #start_time = time.time()
    # Get last_seen and earliest_seen on the narrative for a given user
    data.sort_values(by=["last_seen"], ascending=False, inplace=True)
    earliest_seen = list(data.last_seen)[-1]
    latest_seen = list(data.last_seen)[0]
    time_delta = (latest_seen - earliest_seen)
    hours = (time_delta.total_seconds())/3600
    # Convert date to datetime format Y-m-d
    date = datetime.datetime.strptime(str(earliest_seen), '%Y-%m-%d %H:%M:%S').replace(minute=0, hour=0, second=0)
    # Get date and ip error tag as string 
    date = str(date)[0:10]
    # If an Ip error tag appears in the data, we need to separate the dictionaries to data without ip errors and those with
    if "tags" in data.columns:
        
        tag = str(list(data.tags)[0])

        if tag == "nan":
            user_activity_dictionary = {"username": user, "date": date,"hours_on_system": hours, "last_seen": latest_seen, "first_seen" : earliest_seen,
                                        "ip_address": ip,"country_name": list(data["country_name"])[0], "country_code":  list(data["country_code"])[0],
                                        "region_name": list(data["region_name"])[0], "region_code": list(data["region_code"])[0],
                                        "city": list(data["city_name"])[0], "postal_code": list(data["postal_code"])[0], "timezone": list(data["timezone"])[0],
                                        "latitude": list(data["latitude"])[0], "longitude": list(data["longitude"])[0],
                                        "host_ip": list(data["host"])[0], "proxy_target": list(data["proxy_target"])[0]}

        else:
            user_activity_dictionary = {"username": user, "date": date,"hours_on_system": hours, "last_seen": latest_seen, "first_seen" : earliest_seen,
                                        "ip_address": tag,"host_ip": list(data["host"])[0], "proxy_target": list(data["proxy_target"])[0],
                                        "country_name": tag, "country_code":  tag, "region_name": tag, "region_code": tag,
                                        "city": tag, "postal_code": tag, "timezone": tag,
                                        "latitude": None, "longitude": None}
                                       
    else:
        user_activity_dictionary = {"username": user, "date": date,"hours_on_system": hours, "last_seen": latest_seen, "first_seen" : earliest_seen,
                                        "ip_address": ip,"country_name": list(data["country_name"])[0], "country_code":  list(data["country_code"])[0],
                                        "region_name": list(data["region_name"])[0], "region_code": list(data["region_code"])[0],
                                        "city": list(data["city_name"])[0], "postal_code": list(data["postal_code"])[0], "timezone": list(data["timezone"])[0],
                                        "latitude": list(data["latitude"])[0], "longitude": list(data["longitude"])[0],
                                        "host_ip": list(data["host"])[0], "proxy_target": list(data["proxy_target"])[0]}

    #print("Elasticsearch dictionaries took ", time.time() - start_time, " seconds to create")
    return user_activity_dictionary


# Summary dictionary from Elasticsearch data
def elastic_summary_dictionaries(str_date=datetime.datetime.combine(yesterday, datetime.datetime.min.time()), end_date=datetime.datetime.combine(yesterday, datetime.datetime.max.time())):
    """Elastic_summary_dictionaries provides summmary dictionaries of user activity and location information from elatic search.
    Given results that are pulled from elastic it iterates through users and then through a user's IP addresses. For each IP address a users 'last_seen' on the system and 
    'first_seen' on the system are found and the time delta between them taken. Dictionaries are then made record a user's location information, 'last_seen', 'first_seen' and duration active."""
    #start_time = time.time()

    # Pull elastic results, drop duplicates from backtracking timestamps in elastic queries, 
    # and format timestamp to readable datetime format 
    elastic_dictionaries = elasticsearch_pull(str_date, end_date)
    elastic_data_df = pd.DataFrame(elastic_dictionaries)
    elastic_data_df.drop_duplicates(inplace=True)
    elastic_data_df["last_seen"] = pd.to_datetime(elastic_data_df["last_seen"], format='%a %b %d %H:%M:%S %Y')
    # Split query results by day
    DFList = [group[1] for group in elastic_data_df.groupby(elastic_data_df.last_seen.dt.day)]
    user_activity_array = []
    # Iterate for day of elastic results activity picked up by the elasticquery 
    for index, elastic_data in enumerate(DFList):

        # Get list of users to iterate over for day 
        users = list(set(list(elastic_data.session_id)))
        for user in users:
            # Set user condition for data
            user_condition = elastic_data.session_id == user
            user_data = elastic_data[user_condition]
            # Get users ip's and check if used ip's > 1
            unique_ips = set(list(user_data.ip))

            if len(unique_ips) > 1:
                for ip in unique_ips:
                    # Get all results for user on specific ip
                    ip_cond = user_data.ip == ip
                    user_ip_data = user_data[ip_cond]
                    # Check system usage as active
                    system_usage = list(set(list(user_ip_data.last_seen)))
                    if len(system_usage) > 1:
                        # make summary dictionary and append dictionary to data array
                        ip_specfic_dict = make_user_activity_dict(user_ip_data, ip, user)
                        user_activity_array.append(ip_specfic_dict)
                    else:
                        continue
            # else same as above without the ip iteration 
            else:
                system_usage = list(set(list(user_data.last_seen)))
                if len(system_usage) > 1:
                    ip = list(user_data.ip)[0]
                    user_dict = make_user_activity_dict(user_data, ip, user)
                    user_activity_array.append(user_dict)
                else:
                    continue
    #print("Elasticsearch summary dictionaries took ", time.time() - start_time, " seconds to run")
    return user_activity_array

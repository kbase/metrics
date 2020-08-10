from pymongo import MongoClient
from pymongo import ReadPreference
import json as _json
import os
import mysql.connector as mysql
import re
import requests
import time
import datetime

requests.packages.urllib3.disable_warnings()

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]

kb_outreach_events_url = os.environ["KB_OUTREACH_EVENTS_URL"]
sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]
db_connection = mysql.connect(
    host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
)

def quote_strip(string):
    """
    helpe function to strip single leading and trailing quote
    """
    return re.sub(r'^"|"$', '', string)

def get_outreach_events():
    """
    Gets the details for outreach events
    """
    params = (("tqx", "out:csv"), ("sheet", "OUTREACH_EVENTS"))
    response = requests.get(kb_outreach_events_url, params=params)
    if response.status_code != 200:
        print(
            "ERROR - KB OUTREACH EVENTS GOOGLE SHEET RESPONSE STATUS CODE : "
            + str(response.status_code)
        )

        return user_stats_dict
    lines = response.text.split("\n")
    headers = lines.pop(0)
    events = dict()
    for temp_line in lines:
        #print("Temp_line: " + str(temp_line))
        temp_line = quote_strip(temp_line)
        line = temp_line.split('","')
        (event_name, event_date, announcement_date, pre_attendee_list_url, event_type, topic,
         presenters, narrative_urls, duration_hours, app_categories, estimated_attendance,
         location, point_of_contact, feedback_form_url, comments ) = line[:15]
        attendee_list_url = ""
        if pre_attendee_list_url is not None and pre_attendee_list_url.startswith("https://docs.google.com/spreadsheets/"):
            attendee_list_url = pre_attendee_list_url.rsplit("/",1)[0] + "/gviz/tq"
        announcement_used = None
        if announcement_date.strip() == "":
            announcement_used = None
        else:
            announcement_used = announcement_date.strip()
        events[event_name] = {
            "event_date": event_date.strip(),
            "announcement_date": announcement_used,
            "attendee_list_url": attendee_list_url.strip(),
            "event_type": event_type.strip(),
            "topic": topic.strip(),
            "presenters": presenters.strip(),
            "narrative_urls" : narrative_urls.strip(),
            "duration_hours": duration_hours.strip(),
            "app_categories": app_categories.strip(),
            "estimated_attendance": estimated_attendance.strip(),
            "location": location.strip(),
            "point_of_contact": point_of_contact.strip(),
            "feedback_form_url": feedback_form_url.strip(),
            "comments": comments.strip()}
    return events


def upload_events(events):
    """
    Takes the events dict and populates the outreach_events table
    in the metrics MySQL DB.
    """
    total_events = len(events.keys())
    rows_info_inserted = 0
    rows_info_updated = 0
    rows_stats_inserted = 0
    # connect to mysql
#    db_connection = mysql.connect(
#        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
#    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    # get all existing users
    existing_events_info = dict()
    query = (
        "select outreach_event_name, event_date, announcement_date, attendee_list_url, event_type, "
        "topic, presenters, narrative_urls, duration_hours, app_categories, "
        "estimated_attendance, location, point_of_contact, feedback_form_url, comments " 
        "from metrics.outreach_events"
    )
    cursor.execute(query)
    for (
            event_name,
            announcement_date,
            event_date,
            attendee_list_url,
            event_type,
            topic,
            presenters,
            narrative_urls,
            duration_hours,
            app_categories,
            estimated_attendance,
            location,
            point_of_contact,
            feedback_form_url,
            comments,
    ) in cursor:
        existing_events_info[event_name] = {
            "event_date": event_date,
            "announcement_date": announcement_date,
            "attendee_list_url": attendee_list_url,
            "event_type": event_type,
            "topic": topic,
            "presenters": presenters,
            "narrative_urls" : narrative_urls,
            "duration_hours": duration_hours,
            "app_categories": app_categories,
            "estimated_attendance": estimated_attendance,
            "location": location,
            "point_of_contact": point_of_contact,
            "feedback_form_url": feedback_form_url,
            "comments": comments,
        }

    print("Number of existing events:" + str(len(existing_events_info)))

    prep_cursor = db_connection.cursor(prepared=True)
    events_insert_statement = (
        "insert into outreach_events "
        "(outreach_event_name, event_date, announcement_date, attendee_list_url, event_type, "
        "topic, presenters, narrative_urls, duration_hours, app_categories, "
        "estimated_attendance, location, "
        "point_of_contact, feedback_form_url, comments) "
        "values(%s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, %s, %s, %s);"
    )

    update_prep_cursor = db_connection.cursor(prepared=True)
    events_update_statement = (
        "update outreach_events "
        "set event_date = %s, announcement_date = %s,  "
        "attendee_list_url = %s, event_type = %s, topic = %s, presenters = %s, "
        "narrative_urls = %s, duration_hours = %s, "
        "app_categories = %s, estimated_attendance = %s, location = %s, "
        "point_of_contact = %s, feedback_form_url = %s, comments = %s  "
        "where outreach_event_name = %s;"
    )

    new_events_count = 0
    events_updated_count = 0

    for event_name in events:
        # check if new user_info exists in the existing user info, if not insert the record.
        if event_name not in existing_events_info:
            input = (
                event_name,
                events[event_name]["event_date"],
                events[event_name]["announcement_date"],
                events[event_name]["attendee_list_url"],
                events[event_name]["event_type"],
                events[event_name]["topic"],
                events[event_name]["presenters"],
                events[event_name]["narrative_urls"],
                events[event_name]["duration_hours"],
                events[event_name]["app_categories"],
                events[event_name]["estimated_attendance"],
                events[event_name]["location"],
                events[event_name]["point_of_contact"],
                events[event_name]["feedback_form_url"],
                events[event_name]["comments"],
            )
            prep_cursor.execute(events_insert_statement, input)
            new_events_count += 1
        else:
            # Check if anything has changed in the events table, if so update the record
            if not (
                (
                    events[event_name]["event_date"] is None
                    or (events[event_name]["event_date"]
                    == str(existing_events_info[event_name]['event_date']))
                )
                and
                (
                    events[event_name]["announcement_date"] is None
                    or (events[event_name]["announcement_date"]
                    == str(existing_events_info[event_name]['announcement_date']))
                )
                and events[event_name]["attendee_list_url"]
                == existing_events_info[event_name]["attendee_list_url"]   
                and events[event_name]["event_type"]
                == existing_events_info[event_name]["event_type"]                    
                and events[event_name]["presenters"]
                == existing_events_info[event_name]["presenters"]
                    and events[event_name]["topic"]
                == existing_events_info[event_name]["topic"]
                and events[event_name]["narrative_urls"]
                == existing_events_info[event_name]["narrative_urls"]
                and int(events[event_name]["duration_hours"])
                == int(existing_events_info[event_name]["duration_hours"])
                and events[event_name]["app_categories"]
                == existing_events_info[event_name]["app_categories"]
                and int(events[event_name]["estimated_attendance"])
                == int(existing_events_info[event_name]["estimated_attendance"])
                and events[event_name]["location"]
                == existing_events_info[event_name]["location"]
                and events[event_name]["point_of_contact"]
                == existing_events_info[event_name]["point_of_contact"]
                and events[event_name]["feedback_form_url"]
                == existing_events_info[event_name]["feedback_form_url"]
                and events[event_name]["comments"]
                == existing_events_info[event_name]["comments"]
            ):
                input = (
                    events[event_name]["event_date"],
                    events[event_name]["announcement_date"],
                    events[event_name]["attendee_list_url"],
                    events[event_name]["event_type"],
                    events[event_name]["topic"],
                    events[event_name]["presenters"],
                    events[event_name]["narrative_urls"],
                    events[event_name]["duration_hours"],
                    events[event_name]["app_categories"],
                    events[event_name]["estimated_attendance"],
                    events[event_name]["location"],
                    events[event_name]["point_of_contact"],
                    events[event_name]["feedback_form_url"],
                    events[event_name]["comments"],
                    event_name,
                )
                update_prep_cursor.execute(events_update_statement, input)
                events_updated_count += 1
    existing_event_names_set = existing_events_info.keys()
    current_event_names_set = events.keys()
    db_only_event_names = existing_event_names_set - current_event_names_set
    if len(db_only_event_names) > 0:
        print("*****************************")
        print("It appears events were removed or renamed. The following events are in the DB, ")
        print("but are currently not in the outreach events sheet. ")
        print("If they are truly meant to be deleted please contact the DBA.")
        for db_event in db_only_event_names:
            print(str(db_event))
    db_connection.commit()

    print("Number of new events inserted:" + str(new_events_count))
    print("Number of events updated:" + str(events_updated_count))
    return 1

def upload_event_users(events):
    """
    uploads the outreach_event_users
    """
    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)
    
    # Query to check for existing records
    # Existing meeting users dict  Top level key is event_name
    # Value is a set of all the user_names
    existing_event_users_dict = dict()
    query = ("select outreach_event_name, username from metrics.outreach_event_users")
    cursor.execute(query)
    for (event_name, username) in cursor:
        if event_name not in existing_event_users_dict:
            existing_event_users_dict[event_name] = set()
        existing_event_users_dict[event_name].add(username)

    # Make set of a valid usernames
    valid_usernames = set()
    query = ("select username from metrics.user_info")
    cursor.execute(query)
    for (username) in cursor:
        valid_usernames.add(username[0])
    
    prep_cursor = db_connection.cursor(prepared=True)
    event_users_insert_statement = (
        "insert into metrics.outreach_event_users "
        "(outreach_event_name, username) "
        "values( %s, %s )"
    )
        
    total_new_users_all_events = 0
    for event_name in events:
        if events[event_name]["attendee_list_url"] != "":
            if event_name not in existing_event_users_dict:
                existing_event_users_dict[event_name] = set()
            previous_existing_count = len(existing_event_users_dict[event_name])
            new_users_in_event = 0
            old_users_in_event_accounted_for = 0
            params = (("tqx", "out:csv"), ("sheet", "Sheet1"))
            response = requests.get(events[event_name]["attendee_list_url"], params=params)
            if response.status_code != 200:
                print("*****************************")
                print("*****************************")
                print(
                    "ERROR - UNABLE TO OPEN ATTENDEE LIST FOR EVENT: "
                    + str(event_name) + " URL: "
                    + str(events[event_name]["attendee_list_url"])
                    + " sheet named 'Sheet1' - ERROR: "
                    + str(response.status_code)
                )
                continue
            lines = response.text.split("\n")
            valid_lines_count = 0
            event_users_set = set()
            for line in lines:
                elements = line.split(',')            
                username = quote_strip(elements[0])
                username = username.strip()
                if username != "":
                    valid_lines_count += 1
                    if username in event_users_set:
                        print("Event : " + str(event_name) + " has duplicate username : " + str(username))
                    else:
                        event_users_set.add(username)
            new_users = event_users_set.difference(existing_event_users_dict[event_name])
            users_removed_from_list = existing_event_users_dict[event_name].difference(event_users_set)
            if len(users_removed_from_list) > 0:
                # PRINT WARNINGS FOR USERNAME THAT HAVE BEEN REMVED FROM THE SHEET.
                # Possibly want to remove them instead, need to talk with Ben
                print("*****************************")
                print("The following usernames were removed from the google sheet for event: ")
                print(str(event_name) + " but were present in the past. ")
                print("If they need to be removed from the event for real please contact the DBA.")
                for username in users_removed_from_list:
                    print(str(username))
            if len(new_users) > 0:
                invalid_usernames = set()
                for new_user in new_users:
                    # check if usernames are valid                    
                    if new_user in valid_usernames:
                        # These are new users that were not yet an attendee for the event
                        # Insert the new user names
                        total_new_users_all_events += 1
                        new_users_in_event += 1
                        input = (event_name, new_user)
                        prep_cursor.execute(event_users_insert_statement, input)
                    else:
                        invalid_usernames.add(new_user)
                if len(invalid_usernames) > 0:
                    print("*****************************")
                    print("Event attendee list for " + str(event_name) + " has the following invalid usernames:")
                    for invalid_username in invalid_usernames:
                        print(str(invalid_username))
                print("Event : " + str(event_name) + " had " + str(new_users_in_event) + " new users added.")
    print("Across all events " + str(total_new_users_all_events) + " new users added.")    
    db_connection.commit()
    return 1


print("############################################")
print("############################################")
print("############################################")
print("OUTREACH EVENTS Upload (UTC): " + str(datetime.datetime.utcnow()))
start_time = time.time()

events = get_outreach_events()
#print("Events:" + str(events))
upload_events(events)
upload_event_users(events)


print(
    "--- Uploading Outreach event took %s seconds ---"
    % (time.time() - start_time)
)

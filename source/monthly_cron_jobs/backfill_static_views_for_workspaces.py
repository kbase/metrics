import os
import requests
import mysql.connector as mysql

metrics_mysql_password = os.environ["METRICS_MYSQL_PWD"]

sql_host = os.environ["SQL_HOST"]
query_on = os.environ["QUERY_ON"]

requests.packages.urllib3.disable_warnings()

kb_google_analytics_url = os.environ["KB_GOOGLE_ANALYTICS_URL"]

def get_kbase_google_analytics():
    """
    Gets the kbase_google_analytics
    """
    params = (("tqx", "out:csv"), ("sheet", "Monthly"))
    response = requests.get(kb_google_analytics_url, params=params)
    if response.status_code != 200:
        print(
            "ERROR - KBase Google analytics  GOOGLE SHEET RESPONSE STATUS CODE : "
            + str(response.status_code)
        )
        print(
            "KBase Google analytics."
        )
        return user_stats_dict

    # key ws_id -> year -> month -> monthly_page_view
    static_narrative_view_monthly_stats = dict()
    
    lines = response.text.split("\n")
    i = 1;
    found_header_line = False
    for line in lines:
        line_elements = line.split(",")
        first_element = line_elements[0][1:-1].strip()
        if found_header_line:
            print("in if")
            landing_page_elements = first_element.split("/")
            ws_id = int(landing_page_elements[2])
            print("ws_id :" + str(ws_id))
            year = int(line_elements[1][1:-1].strip())            
            print("year :" + str(year))
            month = int(line_elements[2][1:-1].strip())
            print("month :" + str(month))            
            page_views = int(line_elements[3][1:-1].strip())
            print("page_views :" + str(page_views))

            if ws_id not in static_narrative_view_monthly_stats:
                static_narrative_view_monthly_stats[ws_id] = dict()
            if year not in static_narrative_view_monthly_stats[ws_id]:
                static_narrative_view_monthly_stats[ws_id][year] = dict()
            if month not in static_narrative_view_monthly_stats[ws_id][year]:
                static_narrative_view_monthly_stats[ws_id][year][month] = 0
            static_narrative_view_monthly_stats[ws_id][year][month] = static_narrative_view_monthly_stats[ws_id][year][month] + page_views
            print(str(i) + " :: " + line)
            i += 1
        if first_element == "Landing Page":
            found_header_line = True
            next


    
    print(str(static_narrative_view_monthly_stats))
    print("Length static_narrative_view_monthly_stats : " + str(len(static_narrative_view_monthly_stats)))
    
    static_narrative_view_summary_stats = dict()
    for ws_id in static_narrative_view_monthly_stats:
        running_total_page_views = 0
        if ws_id not in static_narrative_view_summary_stats:
            static_narrative_view_summary_stats[ws_id] = dict()
        for year in sorted(static_narrative_view_monthly_stats[ws_id]):
            if year not in static_narrative_view_summary_stats[ws_id]:
                static_narrative_view_summary_stats[ws_id][year] = dict()
            for month in sorted(static_narrative_view_monthly_stats[ws_id][year]):
                running_total_page_views = running_total_page_views + static_narrative_view_monthly_stats[ws_id][year][month]
                static_narrative_view_summary_stats[ws_id][year][month] = running_total_page_views
                
    print(str(static_narrative_view_summary_stats))
    print("Length static_narrative_view_summary_stats : " + str(len(static_narrative_view_summary_stats)))


#####################################
    
    years_to_do = [2020,2021,2022,2023]
    months_to_do = [1,2,3,4,5,6,7,8,9,10,11,12]
    static_narrative_view_complete_stats = dict()
    for ws_id in static_narrative_view_monthly_stats:
        running_total_page_views = 0
        if ws_id not in static_narrative_view_complete_stats:
            static_narrative_view_complete_stats[ws_id] = dict()
            for year in years_to_do:
#                if year in static_narrative_view_monthly_stats[ws_id]:
                if year not in static_narrative_view_complete_stats[ws_id]:
                    static_narrative_view_complete_stats[ws_id][year] = dict()
                for month in months_to_do:
                    if year not in static_narrative_view_monthly_stats[ws_id] or month not in static_narrative_view_monthly_stats[ws_id][year]:
                        static_narrative_view_complete_stats[ws_id][year][month] = dict()
                    if year in static_narrative_view_monthly_stats[ws_id] and month in static_narrative_view_monthly_stats[ws_id][year]:
                        running_total_page_views = running_total_page_views + static_narrative_view_monthly_stats[ws_id][year][month]
                    static_narrative_view_complete_stats[ws_id][year][month] = running_total_page_views

    print(str(static_narrative_view_complete_stats))
    print("Length static_narrative_view_complete_stats : " + str(len(static_narrative_view_complete_stats)))


##########################

    # connect to mysql
    db_connection = mysql.connect(
        host=sql_host, user="metrics", passwd=metrics_mysql_password, database="metrics"
    )

    cursor = db_connection.cursor()
    query = "use " + query_on
    cursor.execute(query)

    update_prep_cursor = db_connection.cursor(prepared=True)
    
    udate_narratives_views_statement = (
        "update metrics.workspaces set static_narratives_views = %s "
        "where ws_id = %s and DATE_FORMAT(`record_date`,'%Y-%m') = %s;"
    )

#    udate_narratives_views_statement = (
#        "update metrics.workspaces set static_narratives_views = %s "
#        "where ws_id = %s and DATE_FORMAT(`record_date`,'%Y') = %s and DATE_FORMAT(`record_date`,'%m') = %s;"
#    )

    updates_performed = 0
    
    for ws_id in static_narrative_view_complete_stats:
 #       if ws_id != 15253:
 #           continue
        for year in static_narrative_view_complete_stats[ws_id]:
            for month in static_narrative_view_complete_stats[ws_id][year]:
                temp_month = month + 1
                temp_year = year
                if temp_month == 13:
                    temp_month = 1
                    temp_year = year + 1
                month_input = str(temp_month)
                if temp_month < 10:
                    month_input = "0" + str(temp_month)
                date_used = str(temp_year) + "-" + month_input
                # Do update statement
                input = (
                    static_narrative_view_complete_stats[ws_id][year][month],
                    ws_id,
                    date_used,
                    )
#                input = (
#                    static_narrative_view_complete_stats[ws_id][year][month],
#                    ws_id,
#                    str(temp_year),
#                    month_input,
#                    )
                print("udate_narratives_views_statement : " + udate_narratives_views_statement)
                print("input : " + str(input))
                update_prep_cursor.execute(udate_narratives_views_statement, input)
                updates_performed += 1

    db_connection.commit()
                
    print("15253 summary: ========")
    print(str(static_narrative_view_summary_stats[15253]))

    print("15253 complete: ========")
    print(str(static_narrative_view_complete_stats[15253]))

    print("Total updates performed: " + str(updates_performed))
    
    return 1

get_kbase_google_analytics()

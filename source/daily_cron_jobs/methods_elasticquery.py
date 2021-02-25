import requests
from requests.auth import HTTPBasicAuth
import json
import os

elasticsearch_url = os.environ["ELASTICSEARCH_URL"]
elasticsearch_user = os.environ.get("ELASTICSEARCH_USER")
elasticsearch_pwd = os.environ.get("ELASTICSEARCH_PWD")


# Query Elastic for Narrative Data


def retrieve_elastic_response(epoch_intial, epoch_final, search_after_timestamp=[]):
    """ Retrieve_elastic_response generates an elasticsearch query to pull narrative container
        information from Elasticsearch. Given an initial epoch timestamp and final epoch timestamp
        the search pulls data from this range. The 'search_after_timestamp' is given to the query
        after the initial pull. The search_after option in
        the query instructs Elasticsearch to pull all data after the 'search_after' timestamp. """

    elastic_query = {
        "query": {
            "bool": {
                "must": [
                    {"match_all": {}},
                    {
                        "match_phrase": {
                            "type.keyword": {"query": "narrativecontainers"}
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": epoch_intial,
                                "lte": epoch_final,
                                "format": "epoch_millis",
                            }
                        }
                    },
                ],
                "must_not": [{"match_phrase": {"session_id": {"query": "*"}}}],
            }
        },
        "size": 10000,
        "sort": [{"@timestamp": {"order": "desc", "unmapped_type": "boolean"}}],
        "_source": {"excludes": []},
        "aggs": {
            "2": {
                "date_histogram": {
                    "field": "@timestamp",
                    "interval": "30m",
                    "time_zone": "America/Los_Angeles",
                    "min_doc_count": 1,
                }
            }
        },
        "stored_fields": ["*"],
        "script_fields": {},
        "docvalue_fields": ["@timestamp"],
        "highlight": {
            "pre_tags": ["@kibana-highlighted-field@"],
            "post_tags": ["@/kibana-highlighted-field@"],
            "fields": {
                "*": {
                    "highlight_query": {
                        "bool": {
                            "must": [
                                {"match_all": {}},
                                {
                                    "match_phrase": {
                                        "type.keyword": {"query": "narrativecontainers"}
                                    }
                                },
                                {
                                    "range": {
                                        "@timestamp": {
                                            "gte": epoch_intial,
                                            "lte": epoch_final,
                                            "format": "epoch_millis",
                                        }
                                    }
                                },
                            ],
                            "must_not": [
                                {"match_phrase": {"session_id": {"query": "*"}}}
                            ],
                        }
                    }
                }
            },
            "fragment_size": 2147483647,
        },
    }


#    headers = {'Content-type': 'content_type_value'}
    headers = {'Content-type': 'application/json'}

    if not search_after_timestamp:
        narrative_container_query_intial = json.dumps(elastic_query)
        response = requests.get(elasticsearch_url,
                                auth=HTTPBasicAuth(elasticsearch_user, elasticsearch_pwd),
                                verify=False,
                                headers=headers,
                                data=narrative_container_query_intial,
                                )
        results_initial = json.loads(response.text)

        return results_initial

    if search_after_timestamp:
        elastic_query["search_after"] = search_after_timestamp
        narrative_container_query_after = json.dumps(elastic_query)
        response = requests.get(elasticsearch_url,
                                auth=HTTPBasicAuth(elasticsearch_user, elasticsearch_pwd),
                                verify=False,
                                headers=headers,
                                data=narrative_container_query_after)
        results_after = json.loads(response.text)

        return results_after

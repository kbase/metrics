
import requests
import json
import pprint

def elasticsearch_pull():

    narrative_container_query = json.dumps({
        "query": {
            "bool": {
                "must": [
                    {
                        "match_all": {}
                    },
                    {
                        "match_phrase": {
                            "type.keyword": {
                                "query": "narrativecontainers"
                            }
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": 1566425440145,
                                "lte": 1566426340145,
                                "format": "epoch_millis"
                            }
                        }
                    }
                ],
                "must_not": [
                    {
                        "match_phrase": {
                            "session_id": {
                                "query": "*"
                            }
                        }
                    }
                ]
            }
        },
        "size": 500,
        "sort": [
            {
                "_score": {
                    "order": "desc"
                }
            }
        ],
        "_source": {
            "excludes": []
        },
        "aggs": {
            "2": {
                "date_histogram": {
                    "field": "@timestamp",
                    "interval": "30s",
                    "time_zone": "America/Los_Angeles",
                    "min_doc_count": 1
                }
            }
        },
        "stored_fields": [
            "*"
        ],
        "script_fields": {},
        "docvalue_fields": [
            "@timestamp"
        ],
        "highlight": {
            "pre_tags": [
                "@kibana-highlighted-field@"
            ],
            "post_tags": [
                "@/kibana-highlighted-field@"
            ],
            "fields": {
                "*": {
                    "highlight_query": {
                        "bool": {
                            "must": [
                                {
                                    "match_all": {}
                                },
                                {
                                    "match_phrase": {
                                        "type.keyword": {
                                            "query": "narrativecontainers"
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "@timestamp": {
                                            "gte": 1566425440145,
                                            "lte": 1566426340145,
                                            "format": "epoch_millis"
                                        }
                                    }
                                }
                            ],
                            "must_not": [
                                {
                                    "match_phrase": {
                                        "session_id": {
                                            "query": "*"
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            },
            "fragment_size": 2147483647
        }

    })

    response = requests.get("http://elasticsearch1.chicago.kbase.us:9200/logstash-narrativecontainers-*/_search", data=narrative_container_query)
    results = json.loads(response.text)
    data = [doc for doc in results['hits']['hits']]
    entries_1 = ('type', 'instance', '@version', 'index', 'geoip')
    entries_2 = ('highlight', 'fields', 'location', '_score', '_index', '_source', '_type')
    data_formatted = []
    #pprint.pprint(data

    for doc in data:
        source_dictionary =  doc['_source']
        
        if 'geoip' in source_dictionary:
            geoip_items = source_dictionary['geoip']
            
            for key in entries_1:
                if key in source_dictionary:
                    del source_dictionary[key]
            
            epoch_timestamp = doc['fields']['@timestamp'][0]
            doc.update(geoip_items)
            doc.update(source_dictionary)

            for key in entries_2:
                if key in doc:
                    del doc[key]

            doc['epoch_timestamp'] = epoch_timestamp
            data_formatted.append(doc)
        else:
            continue

    #pprint.pprint(data_formatted)
    return data_formatted

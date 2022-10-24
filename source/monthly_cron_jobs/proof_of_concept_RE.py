from arango import ArangoClient

re_client = ArangoClient(host="http://graph1:8531")
# Connect to "test" database as root user.
re_db = re_client.db('prod', username='jkbaumohl', password='neigh-meshwork-plaque')

# Get the AQL API wrapper.
aql = re_db.aql


re_cursor = re_db.aql.execute(
      "FOR doc IN ws_prov_descendant_of LET fullid =  CONCAT("ws_object_version/", id)
      'FOR doc IN students FILTER doc.age < @value RETURN doc',
      bind_vars={'value': 19}
    )






cursor = re_db.aql.execute('FOR doc IN students RETURN doc')

sourcelist = ["112263:4:1","112263:2:1"]

for id in sourcelist
    FOR doc IN ws_prov_descendant_of
          LET fullid =  CONCAT("ws_object_version/", id)
                FILTER doc._to==fullid
                      RETURN { "input object": fullid, "resulting object": doc._from}

from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=["http://elasticsearch:9200"])

# Test the connection
if es.ping():
    print("Successfully connected to Elasticsearch!")
else:
    print("Could not connect to Elasticsearch.")


# Perform a 'match_all' query to fetch all documents from your index (replace 'my_index' with your actual index name)
response = es.search(index="my_index", body={
    "query": {
        "match_all": {}
    }
})


# Print the retrieved documents
for hit in response['hits']['hits']:
    print(hit['_source'])
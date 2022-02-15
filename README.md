germainAPM Elasticsearch Plugin
=========================================

This plugin adds the ability to compute percentile values for aggregated data. 
Given an index with one or more fields holding serialized t-digest values, 
the plugin provides a new value aggregation type `digest` which will return 
a merged digest value for each bucket. 

Installation
------------

`bin/elasticsearch-plugin install "https://github.com/germainsoftware/germain-es-plugin/releases/download/v0.5.0-SNAPSHOT/germainapm-es-plugin-0.5.0-SNAPSHOT.zip"`

Build
-----
Requires Java 11

Digest aggregation
--------------------------

### Parameters

 - `field` : `binary` field to aggregate on. 
    Note: In order to aggregate a `binary` field, it has to be added to `doc_values`.

Examples
-------

#### Index holding aggregated data 

```
# Add data:

PUT my-index
{
  "mappings": {
    "properties": {
      "entryCount": {
        "type": "long"
      },
      "name": {
        "type": "keyword"
      },
      "value.digest": {
        "type": "binary",
        "doc_values": true
      },
      "value.sum": {
        "type":"double"
      }
    }
  }
}

PUT /my-index/_doc/1
{
  "entryCount": 15,
  "name": "Web Traffic",
  "value.digest": "AAAAAT/E3S8an753P8euFHrhR65AJAAAAAAAAAAAAAI/8AAAAAAAAD/E3S8an753P/AAAAAAAAA/x64UeuFHrg==",
  "value.sum": 2475
}

PUT /my-index/_doc/2
{
  "entryCount": 28,
  "name": "File Downloads",
  "value.digest": "AAAAAT+xaHKwIMScP+mJN0vGp/BAJAAAAAAAAAAAAAI/8AAAAAAAAD+xaHKwIMScP/AAAAAAAAA/6Yk3S8an8A==",
  "value.sum": 2938
}

PUT /my-index/_doc/3
{
  "entryCount": 2839,
  "name": "Web Crawler",
  "value.digest": "AAAAAT/ci0OVgQYlP91wo9cKPXFAJAAAAAAAAAAAAAI/8AAAAAAAAD/ci0OVgQYlP/AAAAAAAAA/3XCj1wo9cQ==",
  "value.sum": 12993
}


# Digest aggregation request :

POST /my-index/_search
{
    "size": 0,
    "aggregations": {
        "value_sum": { "sum": {"field": "value.sum" } },
        "value_digest":{ "digest": { "field":"value.digest" } }
    }
}


Result :

{
  "hits": {
    "total": {
      "value": 3,
      "relation": "eq"
    }
  },
  "aggregations": {
    "value_sum": {
      "value": 18406.0
    },
    "value_digest": {
      "value": "AAAAAT+xaHKwIMScP+mJN0vGp/BAJAAAAAAAAAAAAAY/8AAAAAAAAD+xaHKwIMScP/AAAAAAAAA/xN0vGp++dz/wAAAAAAAAP8euFHrhR64/8AAAAAAAAD/ci0OVgQYlP/AAAAAAAAA/3XCj1wo9cT/wAAAAAAAAP+mJN0vGp/A="
    }
  }
}

```

License
-------

This software is under The MIT License (MIT).

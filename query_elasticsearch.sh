# -----------------------------------------------------------------------------------------

# Provider        Elastic Cloud
# Link            https://cloud.elastic.co/
# Username        brad.murray@contactenergy.co.nz
# Password        beXY32uZQUR4RzFuY7R2

# Name            energy-net
# Version         v6.1.4
# Elasticsearch   https://223eef68581513e2342163ce638542d1.ap-southeast-2.aws.found.io:9243
# Kibana          https://41bab40a3ead1a59adcfe1a09a7a8803.ap-southeast-2.aws.found.io:9243
# Username        elastic
# Password        KVKHdj8U5TDvPhzX229EdHXe

# CLUSTER_ID      223eef68581513e2342163ce638542d1
# REGION          ap-southeast-2
# CLOUD_PLATFORM  aws
# DOMAIN          found.io
# PORT            9243

# -----------------------------------------------------------------------------------------

# Defining the path
data_directory=/scratch-network/projects/2019/EPEC2/data/
# data_directory=/data/data-uc/data-epec2
mkdir -p $data_directory
cd $data_directory



# -----------------------------------------------------------------------------------------

# Get indexes (databases)

curl \
  -u elastic:KVKHdj8U5TDvPhzX229EdHXe \
  -H "Content-Type: application/json" \
  -XGET "https://223eef68581513e2342163ce638542d1.ap-southeast-2.aws.found.io:9243/_cat/indices?v&s=index" \
  > ./indices.txt

# Get mapping

curl \
  -u elastic:KVKHdj8U5TDvPhzX229EdHXe \
  -H "Content-Type: application/json" \
  -XGET "https://223eef68581513e2342163ce638542d1.ap-southeast-2.aws.found.io:9243/_mapping?pretty" \
  > ./mapping.json

# Counts

#Index document counts
curl \
  -u elastic:KVKHdj8U5TDvPhzX229EdHXe \
  -H "Content-Type: application/json" \
  -XGET "https://223eef68581513e2342163ce638542d1.ap-southeast-2.aws.found.io:9243/_search" \
  -d'
    {
      "aggs": {
        "indicesAgg": {
          "terms": {
            "field": "_index",
            "size": 10000
          }
        }
      },
      "size": 0
    }
  ' \
  | jq . \
  > /scratch-network/projects/2019/EPEC2/data/index_document_counts.json

#Ty[pe document counts
curl \
  -u elastic:KVKHdj8U5TDvPhzX229EdHXe \
  -H "Content-Type: application/json" \
  -XGET "https://223eef68581513e2342163ce638542d1.ap-southeast-2.aws.found.io:9243/_search" \
  -d'
    {
      "aggs": {
        "typesAgg": {
          "terms": {
            "field": "_type",
            "size": 10000
          }
        }
      },
      "size": 0
    }
  ' \
  | jq . \
  > ./type_document_counts.json

#Index and Type document counts
curl \
  -u elastic:KVKHdj8U5TDvPhzX229EdHXe \
  -H "Content-Type: application/json" \
  -XGET "https://223eef68581513e2342163ce638542d1.ap-southeast-2.aws.found.io:9243/_search" \
  -d'
    {
      "aggs": {
        "indicesAgg": {
          "terms": {
            "field": "_index",
            "size": 10000
          },
          "aggs": {
            "typesAgg": {
              "terms": {
                "field": "_type",
                "size": 10000
              }
            }
          }
        }
      },
      "size": 0
    }
  ' \
  | jq . \
  > ./index_and_type_document_counts.json

# -----------------------------------------------------------------------------------------



# -----------------------------------------------------------------------------------------

# Download data example

curl \
  -u elastic:KVKHdj8U5TDvPhzX229EdHXe \
  -H "Content-Type: application/json" \
  -XGET "https://223eef68581513e2342163ce638542d1.ap-southeast-2.aws.found.io:9243/embrium-2017-07/_search?scroll=1m" \
  -d'
    {
      "size" : 1,
      "query": {
        "match_all": {}
      }
    }
  ' | jq . 

#Output

# {
#   "_scroll_id": "DnF1ZXJ5VGhlbkZldGNoBQAAAAAz88fmFlNJd09reFV1U3ktVnJ3Ti1xSjRubkEAAAAAM_PH5xZTSXdPa3hVdVN5LVZyd04tcUo0bm5BAAAAADMOCiAWRGdGaDV3eVRUNFNxNWNBMzY4TWlZZwAAAAAz88flFlNJd09reFV1U3ktVnJ3Ti1xSjRubkEAAAAAM_PH6BZTSXdPa3hVdVN5LVZyd04tcUo0bm5B",
#   "took": 3,
#   "timed_out": false,
#   "_shards": {
#     "total": 5,
#     "successful": 5,
#     "skipped": 0,
#     "failed": 0
#   },
#   "hits": {
#     "total": 149048,
#     "max_score": 1,
#     "hits": [
#       {
#         "_index": "embrium-2017-07",
#         "_type": "readings",
#         "_id": "589a3875b624487b77dfc34e:frequency:1499865000077",
#         "_score": 1,
#         "_source": {
#           "date": "2017-07-13T01:10:00.077Z",
#           "inserted": "2017-07-13T01:10:04.860Z",
#           "connector": "589a313eb624487b77dfc340",
#           "icp": "0000150476TRBF5",
#           "channel": "589a3875b624487b77dfc34e",
#           "property": "frequency",
#           "siteName": "0000150476TRBF5",
#           "value": 49.95,
#           "device": "589a3847b624487b77dfc34d",
#           "streamName": "market_freq"
#         }
#       }
#     ]
#   }
# }

#Scroll id given to extract rest of teh data
curl \
  -u elastic:KVKHdj8U5TDvPhzX229EdHXe \
  -H "Content-Type: application/json" \
  -XGET "https://223eef68581513e2342163ce638542d1.ap-southeast-2.aws.found.io:9243/_search/scroll" \
  -d'
    {
      "scroll": "1m",
      "scroll_id": "DnF1ZXJ5VGhlbkZldGNoBQAAAAAzEU7VFkRnRmg1d3lUVDRTcTVjQTM2OE1pWWcAAAAAM_cNQBZTSXdPa3hVdVN5LVZyd04tcUo0bm5BAAAAADP3DUEWU0l3T2t4VXVTeS1WcndOLXFKNG5uQQAAAAAz9w1CFlNJd09reFV1U3ktVnJ3Ti1xSjRubkEAAAAAMxFO1hZEZ0ZoNXd5VFQ0U3E1Y0EzNjhNaVln"      
    }
  ' | jq .

# ...

# -----------------------------------------------------------------------------------------



# -----------------------------------------------------------------------------------------

# Download data scripted

batch_size=10000

# Each indices files has to be given
indices=(
  embrium-2017-07
  embrium-2017-08
  embrium-2017-09
  embrium-2017-10
  embrium-2017-11
  embrium-2017-12
  embrium-2018-01
  embrium-2018-02
  embrium-2018-03
  embrium-2018-04
  embrium-2018-04-01
  embrium-2018-04-02
  embrium-2018-04-03
  embrium-2018-04-04
  embrium-2018-04-05
  embrium-2018-04-06
  embrium-2018-04-07
  embrium-2018-04-08
  embrium-2018-04-09
  embrium-2018-04-10
  embrium-2018-04-11
  embrium-2018-04-12
  embrium-2018-04-13
  embrium-2018-04-14
  embrium-2018-04-15
  embrium-2018-04-16
  embrium-2018-04-17
  embrium-2018-04-18
  embrium-2018-04-19
  embrium-2018-04-20
  embrium-2018-04-21
  embrium-2018-04-22
  embrium-2018-04-23
  embrium-2018-04-24
  embrium-2018-04-25
  embrium-2018-04-26
  embrium-2018-04-27
  embrium-2018-04-28
  embrium-2018-04-29
  embrium-2018-04-30
  embrium-2018-05
  embrium-2018-06
  embrium-2018-06-07
  embrium-2018-06-08
  embrium-2018-07
  embrium-2018-08
  embrium-2018-09
  embrium-2018-10
  embrium-2018-11
  embrium-2018-12
  embrium-2019-01
  embrium-2019-02
  embrium-2019-03
  embrium-2019-04
  embrium-2019-05
  embrium-2019-06
  embrium-2019-07
  embrium-2019-08
  embrium-2019-09
  embrium-2019-10
  embrium-2019-11
)
for index in "${indices[@]}"; 
do 

  echo "$index"
  echo ""

  _scroll_counter=0
  _scroll_counter_padded=$(printf "%05d\n" $_scroll_counter)

  rm -r $data_directory/documents/$index/
  mkdir -p $data_directory/documents/$index/
  cd $data_directory/documents/$index/

  curl \
    -u elastic:KVKHdj8U5TDvPhzX229EdHXe \
    -H "Content-Type: application/json" \
    -s \
    -XGET "https://223eef68581513e2342163ce638542d1.ap-southeast-2.aws.found.io:9243/$index/_search?scroll=10m" \
    --data-binary @- \
    > ./${index}_scroll_${_scroll_counter_padded}.json \
    << EOF
{
  "size" : $batch_size,
  "query": {
    "match_all": {}
  }
}
EOF
  _scroll_id=$(cat ${index}_scroll_00000.json | jq -r ._scroll_id)
  _result_count=$(cat ${index}_scroll_${_scroll_counter_padded}.json | jq '.hits.hits | length')
  _result_total=$_result_count

  echo "${index}_scroll_${_scroll_counter_padded}    $(printf "%5d\n" $_result_count)"

  while [ "$_result_count" -ne 0 ]
  do

    _scroll_counter=$((_scroll_counter + 1))
    _scroll_counter_padded=$(printf "%05d\n" $_scroll_counter)
    curl \
      -u elastic:KVKHdj8U5TDvPhzX229EdHXe \
      -H "Content-Type: application/json" \
      -s \
      -XGET "https://223eef68581513e2342163ce638542d1.ap-southeast-2.aws.found.io:9243/_search/scroll" \
      --data-binary @- \
      > ./${index}_scroll_${_scroll_counter_padded}.json \
      << EOF
{
  "scroll": "10m",
  "scroll_id": "$_scroll_id"      
}
EOF

    _result_count=$(cat ${index}_scroll_${_scroll_counter_padded}.json | jq '.hits.hits | length')
    _result_total=$((_result_total + _result_count))

    echo "${index}_scroll_${_scroll_counter_padded}    $(printf "%5d\n" $_result_count)"

  done

  rm ./${index}_scroll_${_scroll_counter_padded}.json

  echo ""
  echo "${index}    $(printf "%5d\n" $_result_total)"
  echo ""

done

# E.g.

# events-2019-11

# events-2019-11_scroll_00000       50
# events-2019-11_scroll_00000       50
# events-2019-11_scroll_00000       50
# events-2019-11_scroll_00000        2
# events-2019-11_scroll_00000        0

# events-2019-11      152

# events-2019-06

# events-2019-06_scroll_00000       50
# events-2019-06_scroll_00000       50
# events-2019-06_scroll_00000       28
# events-2019-06_scroll_00000        0

# events-2019-06      128

# -----------------------------------------------------------------------------------------







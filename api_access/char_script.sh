python3 python_api_thing.py $1 | hdfs dfs -put - /cs455/project/data/chars_with_ids/chars${1}.json

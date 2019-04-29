import json

import urllib.request
import sys

if len(sys.argv) != 2:
    print("error: need an argument for find file #", file=sys.stderr)
    sys.exit()

find_file=open("finds/find"+sys.argv[1]+".json")
find_list=json.load(find_file)

print('[')

for i in range(len(find_list)):
    print("entry " + str(i), file=sys.stderr, end='\r');
    char_url=find_list[i]['charsheet_api']
    id_profile=find_list[i]['id_profile']
    uuid = find_list[i]['uuid']
    if char_url.startswith("/characters/get"):
        char_url='/159332/47a2fab0-5c64-11e9-810c-0200008275de'+char_url
    char_url='http://zigur.te4.org'+char_url
    #print(char_url)
    #with urllib.request.urlopen('http://zigur.te4.org/159332/47a2fab0-5c64-11e9-810c-0200008275de/characters/get/222032/tome/13f27cc9-04f8-4143-9598-74ac676b5251/json') as response:
    with urllib.request.urlopen(char_url) as response:
        #char_data=json.load(response)
        #print(char_data["character"])
        response_json=json.load(response)
        response_json['id_profile']=id_profile
        response_json['uuid']=uuid
        print(json.dumps(response_json))
        if i != len(find_list)-1:
            print(',')


print(']')

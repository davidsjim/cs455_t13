echo "finding page $1 and storing to find$1.json"
echo "zigur.te4.org/159332/47a2fab0-5c64-11e9-810c-0200008275de/characters/find?id_version=16906,31178,28789,40485,12856,42183,13400,13682,12844,18784,17056,109&max=1000&page=$1"
#curl "zigur.te4.org/159332/47a2fab0-5c64-11e9-810c-0200008275de/characters/find?id_version=16906,31178,28789,40485,12856,42183,13400,13682,12844,18784,17056,109&max=1000&page=$1" > finds/find$1.json
curl "zigur.te4.org/159332/47a2fab0-5c64-11e9-810c-0200008275de/characters/find?max=1000&page=$1" > finds/find$1.json

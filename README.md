# cs455_t13

./pyspark --total-executor-cores <num>

To get access to cluster:
iexport HADOOP_CONF_DIR=~milewood/Public/conf

Files:
* **filterChars.py**: a file to test our filtering code
* **functions.py**: a file containing some simple filtering code
* **kmeansCluster(Archer|Archmage).py**: Files with clustering code for a class. Args are num\_clusters, num\_iterations, and print\_cutoff, in that order
* **api\_access/find\_script.sh**: Runs a find request for the api for the given page #, and puts it into a file in api\_access/finds
* **api\_access/python\_api\_thing.py**: prints a series of characters from the given find #
* **api\_access/char\_script.sh**: streams character data from the given find # to the hadoop cluster


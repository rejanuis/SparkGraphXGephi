# Gefx-Generator
Generate file .gexf from database Mongo and Hbase using Graphx Library
This use rabbit mq as queue message for run the job

### HBase
  * HBase table name: sna-graph
  * HBase row id: hashedpostid_typesocmed_date (example: 007d752a62104d6426079747d44400cb_|_ TW_|_20181211)
  * HBase column family name: 0
  * HBase column qualifier name: hashedpost id (example: ce49c16ecd29169ba1b118a88464cbdf)

### Submit job spark #
First before you submit the job, make sure kill the previous spark job on server

	to run this engine, use service spark-submit --class name of class --master yarn/local [name file].jar [path_file properties] [mode database] example :

    '$ spark-submit --class com.research.Main --master local[2] sparkgraphxgephi-1.0-jar-with-dependencies.jar "/home/reja/sparkgraphxgephi/config.properties" "hbase" '

### version 1.0
please contact developer ifgot some probem with run this project (^__^)

# Gefx-Generator
Generate file .gexf from database Mongo and Hbase using Graphx Library
This use rabbit mq as queue message for run the job

# Submit job spark #
First before you submit the job, make sure kill the previous spark job on server

	to run this engine, use service spark-submit --class name of class --master yarn/local [name file].jar [path_file properties] [mode database] example :

    '$ spark-submit --class com.research.Main --master local[2] graphx-1.0-jar-with-dependencies.jar "/home/reja/sparkgraphxgephi/config.properties" "hbase" '

## version 1.0
please contact developer ifgot some probem with run this project \(^__^)\
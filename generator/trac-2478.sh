hadoop fs -test -e "TEST/" && hadoop fs -rmr "TEST/"
hadoop jar /usr/lib/hadoop-0.20/contrib/streaming/hadoop-streaming-0.20.2-cdh3u2.jar \
-Dmapred.compress.map.output=true \
-Dmapred.output.compress=true \
-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
-file "mapper.py" \
-file "reducer.py" \
-input "/heritrix/output/logs/crawl*-2014*/crawl.log*.gz" \
-mapper "mapper.py" \
-reducer "reducer.py" \
-output "TEST/"

#-input "/heritrix/output/logs/crawl0-20140610125756/crawl.log.20141105100930.gz" \

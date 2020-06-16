cluster_host=`hostname -f`
cat /tmp/cluster.props.template | sed -e "s/KAFKA_HOST/$cluster_host/g" -e "s/PHOENIX_HOST/$cluster_host/g" > /tmp/cluster_complete.props                                                                                                                  
                                                                                                                         
hdfs dfs -put /tmp/GeoLite2-City.mmdb /tmp
                                                                                                                         
flink run -m yarn-cluster -d -p 1 -ys 3 -ytm 3G -ynm ZeekPipeline /tmp/log-analytics-hub-1.0-SNAPSHOT.jar /tmp/cluster_complete.props 

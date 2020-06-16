if [ -z ${FLINK_HOST+x}]; then
     echo "FLINK_HOST" is not set
else
     echo "FLINK_HOST" is set to $FLINK_HOST
     scp  src/main/resources/maxmind/GeoLite2-City.mmdb centos@$FLINK_HOST:/tmp
     scp src/main/resources/config/cluster.props.template centos@$FLINK_HOST:/tmp
     scp src/main/resources/scripts/start_flink_job.sh centos@$FLINK_HOST:/tmp
     mvn clean package -DskipTests
     scp target/log-analytics-hub-1.0-SNAPSHOT.jar centos@$FLINK_HOST:/tmp
fi

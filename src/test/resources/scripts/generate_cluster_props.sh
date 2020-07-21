cat src/test/resources/config/cluster.props.template | sed -e "s/KAFKA_HOST/$FLINK_HOST/g" -e "s/PHOENIX_HOST/$FLINK_HOST/g" > src/test/resources/config/cluster.props

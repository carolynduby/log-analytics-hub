export CLUSTER_HOST=ec2-54-177-34-16.us-west-1.compute.amazonaws.com

cat src/test/resources/config/cluster.props.template | sed -e "s/KAFKA_HOST/$CLUSTER_HOST/g" -e "s/PHOENIX_HOST/$CLUSTER_HOST/g" > src/test/resources/config/cluster.props

export CLUSTER_HOST=ec2-18-144-79-43.us-west-1.compute.amazonaws.com

cat src/main/resources/config/cluster.props.template | sed -e "s/KAFKA_HOST/$CLUSTER_HOST/g" -e "s/PHOENIX_HOST/$CLUSTER_HOST/g" > src/main/resources/config/cluster.props 

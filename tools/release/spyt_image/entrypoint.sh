
# COMPAT(atokarew) setting default spark distrinb version to 3.2.2 if it not specified
# Remove after k8s operator will be updated to explicitly specify spark versions
if [ -z "$EXTRA_SPARK_VERSIONS" ]; then
  EXTRA_SPARK_VERSIONS="3.5.6"
fi
# COMPAT end


echo "EXTRA_CONFIG_GENERATOR_OPTIONS = $EXTRA_CONFIG_GENERATOR_OPTIONS"
echo "EXTRA_PUBLISH_CLUSTER_OPTIONS = $EXTRA_PUBLISH_CLUSTER_OPTIONS"
echo "EXTRA_SPARK_VERSIONS = $EXTRA_SPARK_VERSIONS"

python3.12 -m scripts.config_generator /data $EXTRA_CONFIG_GENERATOR_OPTIONS
python3.12 -m scripts.publish_cluster /data $EXTRA_PUBLISH_CLUSTER_OPTIONS
python3.12 -m scripts.spark_distrib $EXTRA_SPARK_VERSIONS

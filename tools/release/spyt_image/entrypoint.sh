# Build spark_distrib arguments from individual env vars (new operator)
SPARK_DISTRIB_ARGS=""
if [ "$SPARK_DISTRIB_OFFLINE" = "true" ]; then
  SPARK_DISTRIB_ARGS="--use-cache --offline"
fi
if [ -n "$SPARK_DISTRIB_VERSIONS" ]; then
  SPARK_DISTRIB_ARGS="$SPARK_DISTRIB_ARGS $SPARK_DISTRIB_VERSIONS"
fi

# COMPAT: support old operator that sends everything in EXTRA_SPARK_DISTRIB_PARAMS
if [ -n "$EXTRA_SPARK_DISTRIB_PARAMS" ]; then
  SPARK_DISTRIB_ARGS="$EXTRA_SPARK_DISTRIB_PARAMS"
fi
# COMPAT end

# Default spark distrib version
if [ -z "$SPARK_DISTRIB_ARGS" ]; then
  SPARK_DISTRIB_ARGS="3.5.7"
fi


echo "EXTRA_CONFIG_GENERATOR_OPTIONS = $EXTRA_CONFIG_GENERATOR_OPTIONS"
echo "EXTRA_PUBLISH_CLUSTER_OPTIONS = $EXTRA_PUBLISH_CLUSTER_OPTIONS"
echo "SPARK_DISTRIB_ARGS = $SPARK_DISTRIB_ARGS"

python3.12 -m scripts.config_generator /data $EXTRA_CONFIG_GENERATOR_OPTIONS
python3.12 -m scripts.publish_cluster /data $EXTRA_PUBLISH_CLUSTER_OPTIONS
python3.12 -m scripts.spark_distrib $SPARK_DISTRIB_ARGS

#!/usr/bin/env bash

export ARROW_PRE_0_15_IPC_FORMAT=1

if [ -z "$SPYT_ROOT" ] && [ -n "$SPARK_CONF_DIR" ]; then
  SPYT_ROOT=$(cd "$SPARK_CONF_DIR"/..; pwd)
  export SPYT_ROOT
fi

if [ -z "$SPYT_CLASSPATH" ] && [ -n "$SPYT_ROOT" ]; then
  SPYT_CLASSPATH="$SPYT_ROOT/jars/*"
  export SPYT_CLASSPATH
fi

javaagent_parameter="-javaagent:$(ls ${SPYT_CLASSPATH}spark-yt-spark-patch*)"

if [ -n "$SPYT_CLASSPATH" ] && [ ! -f "$SPARK_CONF_DIR/java-opts" ]; then
  SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS $javaagent_parameter"
  export SPARK_SUBMIT_OPTS
fi

if [ -z "$SPARK_LAUNCHER_OPTS" ] && [ -n "$SPARK_SUBMIT_OPTS" ]; then
  SPARK_LAUNCHER_OPTS=$SPARK_SUBMIT_OPTS
fi

if [ -z "$SPARK_LAUNCHER_OPTS" ]; then
  SPARK_LAUNCHER_OPTS=$javaagent_parameter
fi

SPARK_LAUNCHER_OPTS="$SPARK_LAUNCHER_OPTS -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED"

export SPARK_LAUNCHER_OPTS

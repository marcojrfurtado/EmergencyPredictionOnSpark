#!/bin/bash

ERROR_CODE=-1

if [ -z "$SPARK_HOME" ]; then
	echo "Environment variable SPARK_HOME has not been set."
	exit $ERROR_CODE
fi

if [ -z "$MASTER" ]; then
	echo "Environment variable MASTER has not been set."
	exit $ERROR_CODE
fi

LOCAL_DIR=$(pwd)
export JAR_LIST="$LOCAL_DIR/utils/lib/sparkling-water-assembly-1.6.5-all.jar,$LOCAL_DIR/utils/lib/gpu-enabler_2.10-1.0.0.jar,$LOCAL_DIR/utils/target/scala-2.10/utils_2.10-0.1-SNAPSHOT.jar"


$SPARK_HOME/bin/spark-shell \
	--master $MASTER \
	--jars $JAR_LIST \
	--deploy-mode client \
	-i $LOCAL_DIR/firePrevention.scala

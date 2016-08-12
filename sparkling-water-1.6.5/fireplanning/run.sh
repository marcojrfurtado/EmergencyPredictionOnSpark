#!/bin/bash

BASE_SPARKLY=".."
LOCAL_DIR=$(pwd)
export SPARKJAREXPORT=,$LOCAL_DIR/utils/target/scala-2.10/utils_2.10-0.1-SNAPSHOT.jar


if [ -n "$HOME_SPARKLY" ]; then
	BASE_SPARKLY=$HOME_SPARKLY
fi
$BASE_SPARKLY/bin/sparkling-shell \
	--conf spark.driver.memory=40g \
       	--conf spark.executor.memory=40g \
	-i $LOCAL_DIR/firePrevention.scala

#!/bin/bash

ERROR_CODE=-1

if [ -z "$HADOOP_HOME" ]; then
	echo "Environment variable HADOOP_HOME has not been set."
	exit $ERROR_CODE
fi





$HADOOP_HOME/bin/hdfs \
	dfs -mkdir seattle-data
$HADOOP_HOME/bin/hdfs \
	dfs -put seattle-data/* seattle-data

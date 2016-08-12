#!/bin/bash

BASHRC_FILE=~/.bashrc

# Configure environment variables corrrectly
echo 'export SPARK_HOME=~/spark-1.6.0-hadoop-2.4.0' >> $BASHRC_FILE
echo 'export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH' >>$BASHRC_FILE

source $BASHRC_FILE

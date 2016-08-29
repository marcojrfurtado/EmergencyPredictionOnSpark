# OpenPower- Emergency Prediction using Spark

An estimated 240 million calls to 911 are made each year, in the U.S alone. Firefighters constitute a significant number of first responders to these emergency calls. 
Predicting frequency of emergency events is extremely useful, as it allows better fire deparment logistics. This incurs in not only a reductions of costs, but it also helps saving more lives.

This project demontrates how deep learning can be used to predict amount of emergency calls, given different types of emergency events and multiple fire stations. 
Our method relies on Apache Spark for dataprocessing, making it possible to scale across multiple machines. We also provide optional GPU processing for some operations.

Data used for our experiments is public, and are made available through [Seattle Open Data portal](https://data.seattle.gov/).
The datasets used in our experiments are the following:

* [Seattle Real-Time Fire 911 Calls](https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj)
* [My Neighborhood Map](https://data.seattle.gov/Community/My-Neighborhood-Map/82su-5fxf)


## Requirements

This package is compatible with Spark 1.5+ and scala 2.10


| Spark Version |  Scala Version | Compatible version of Spark GPU |
| ------------- |-----------------|----------------------|
| `1.5+`        | `2.10`          |`1.0.0`               |


GPU requirements are the following:


| CUDA Toolkit Version |
| -------------------- |
| `7.5`                |


## Setup

Before running this project, set following environment variables accordingly:


| Variable      |  Value                                | Example                                |
| ------------- |---------------------------------------|----------------------------------------|
| `SPARK_HOME`  | `<path to Apache Spark installation>` |`/home/user/apache-spark`               |
| `MASTER`      | `<URL to SPARK Master>`               |`spark://192.168.0.1:7077`              |
| `HADOOP_HOME` | `<path to Apache Hadoop installation>`|`/home/user/hadoop`                     |


## Building

(Note: Not necessary if using pre-compiled release version)

Requires [SBT](http://www.scala-sbt.org/).
Also make sure `nvcc` is visible through the PATH environment variable.

Just excute the following commands:

* `cd fireplanning`
* `./build.sh`

## Uploading Seattle emergency data to Hadoop

This step is necessary before running this application. 
Make sure `HADOOP_HOME` has been correctly set before uploading data

* `cd fireplanning`
* `./submit_data.sh`

## Running

After uploading data into Hadoop, run with the following commands:

* `cd fireplanning`
* `./run.sh`

This shell script will run our main application through Spark Shell in interactive mode. 
This means that after execution, the application will not exit, and scala terminal will be available to user.

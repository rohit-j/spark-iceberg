#!/bin/bash

if [ -z "$1" ]; then
    echo "Error: No spark dataload config file path provided."
    echo "Usage: $0 <filepath>"
    exit 1
fi
sparkPath=$(dirname "$(realpath "$0")")
echo $sparkPath
# -Xmx128g
java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED -cp "$sparkPath/custom:$sparkPath/spark-IcebergLoad-1.0-SNAPSHOT.jar:$sparkPath/dataflow-IcebergLoad-1.0-SNAPSHOT-jar-with-dependencies.jar:$sparkPath/libs/*:" com.spark.iceberg.Load "$1"
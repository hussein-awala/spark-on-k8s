#!/bin/bash

STORAGE_BACKEND=$1

if [ "$STORAGE_BACKEND" == "local" ]; then
    cat <<EOF > ivy.xml
<ivy-module version="2.0">
    <info organisation="com.spark-on-k8s" module="spark-history"/>
    <dependencies>
    </dependencies>
</ivy-module>
EOF
elif [ "$STORAGE_BACKEND" == "s3" ]; then
    cat <<EOF > ivy.xml
<ivy-module version="2.0">
    <info organisation="com.spark-on-k8s" module="spark-history"/>
    <dependencies>
        <dependency org="org.apache.hadoop" name="hadoop-aws" rev="${HADOOP_VERSION}"/>
    </dependencies>
</ivy-module>
EOF
else
    echo "Unsupported storage backend: $STORAGE_BACKEND"
    exit 1
fi

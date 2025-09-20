#!/bin/bash
spark-submit \
  --master spark://spark:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-avro_2.12:3.5.0 \
  /opt/spark-apps/stream_processor/main.py
#!/bin/bash

tags=("Version" "PackageVersion")
tagsAssembly=("AssemblyVersion" "FileVersion")
files=(
  "core/Streamiz.Kafka.Net.csproj"
  "metrics/Streamiz.Kafka.Net.Metrics.Prometheus/Streamiz.Kafka.Net.Metrics.Prometheus.csproj"
  "metrics/Streamiz.Kafka.Net.Metrics.OpenTelemetry/Streamiz.Kafka.Net.Metrics.OpenTelemetry.csproj"
  "serdes/Streamiz.Kafka.Net.SchemaRegistry.SerDes/Streamiz.Kafka.Net.SchemaRegistry.SerDes.csproj"
  "serdes/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro.csproj"
  "serdes/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf.csproj"
  "serdes/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json.csproj"
  "serdes/Streamiz.Kafka.Net.SerDes.CloudEvents/Streamiz.Kafka.Net.SerDes.CloudEvents.csproj"
  "test/Streamiz.Kafka.Net.Tests/Streamiz.Kafka.Net.Tests.csproj"
  "remote/Streamiz.Kafka.Net.Azure.RemoteStorage/Streamiz.Kafka.Net.Azure.RemoteStorage.csproj"
  )

for tag in ${tags[@]}; do
  for file in ${files[@]}; do
  sed -i '' -e "s/<$tag>\(.*\)<\/$tag>/<$tag>$1<\/$tag>/g" $file
  done
  echo "$tag done"
done

for tag in ${tagsAssembly[@]}; do
  for file in ${files[@]}; do
  version=$1
  newVersion="${version//-RC[0-9]*/}" 
  sed -i '' -e "s/<$tag>\(.*\)<\/$tag>/<$tag>$newVersion<\/$tag>/g" $file
  done
  echo "$tag done"
done
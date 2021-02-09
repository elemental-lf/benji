#!/bin/bash

set -euo pipefail

function mc 
{
  port=$1; shift
  command="$@"
  
  docker run --rm --net=host --entrypoint=/bin/sh minio/mc -c "/usr/bin/mc -q --insecure config host add s3 http://localhost:$port/ minio minio123; /usr/bin/mc -q --insecure $command"
}

mc 9901 mb s3/benji
mc 9901 mb s3/benji-2

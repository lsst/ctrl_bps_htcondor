#!/bin/bash

# Args: name DAG_STATUS RETURN
# redirect stdout/stderr to file
{
   echo "DAG post args = $@"
   echo $2 > ${1}.status.txt
   exit 0
} 2>&1 > ${1}.dag.post.out

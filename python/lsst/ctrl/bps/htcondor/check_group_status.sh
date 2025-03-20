#!/bin/sh

pwd

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 GROUP_STATUS_FILE" >&2
    exit 1
fi

if ! [ -e "$1" ]; then
    echo "$1 not found" >&2
    exit 1
fi

group_status=$(<"$1")
echo "Read status ${group_status} from group status file ($1)" >&2
exit ${group_status}

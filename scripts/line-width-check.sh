#!/bin/bash -e

THIS_DIR="$(dirname $(readlink -f $0))"
count=0
for file in $THIS_DIR/../src/*.erl; do
  lines=$(cat $file | awk '{if(length($0) > 80){printf "%d %s\\n", NR, $0;}}')
  if [ -n "$lines" ]; then
    echo $file
    echo -e $lines
    count=$((count + 1))
  fi
done
exit $count


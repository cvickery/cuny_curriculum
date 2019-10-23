#! /usr/local/bin/bash

# Check that all queries that check_queries checks are actually referenced by a python script
# somewhere in this directory.

ok='ok'
for query in `./check_queries.py --list`
do ack "$query" *.py > /dev/null
  [[ $? != 0 ]] &&  ok='not ok' echo $query 'not referenced in *.py'
done
echo $ok

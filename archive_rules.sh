#! /usr/local/bin/bash

SECONDS=0

echo -n "source_courses ... "
psql -Xqd cuny_curriculum -c "copy (select  rule_key(rule_id) as rule_key, \
                              course_id, \
                              offer_nbr, \
                              min_credits, \
                              max_credits, \
                              credits_source, \
                              min_gpa, \
                              max_gpa \
                      from source_courses) to \
                      '`pwd`/rules_archive/`date +"%Y-%m-%d"`_source_courses.csv' csv"
echo done

echo -n "destination_courses ... "
psql -Xqd cuny_curriculum -c "copy (select  rule_key(rule_id) as rule_key, \
                              course_id, \
                              offer_nbr, \
                              transfer_credits \
                      from destination_courses) to \
                      '`pwd`/rules_archive/`date +"%Y-%m-%d"`_destination_courses.csv' csv"
echo done

echo $SECONDS seconds
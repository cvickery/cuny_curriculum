#! /usr/local/bin/bash

SECONDS=0
update_date=`psql -Xqtd cuny_curriculum \
                  -c "select update_date from updates where table_name='transfer_rules'"`
update_date=${update_date// /}
echo Archiving $update_date

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
                      '`pwd`/rules_archive/${update_date}_source_courses.csv' csv"
echo done

echo -n "destination_courses ... "
psql -Xqd cuny_curriculum -c "copy (select  rule_key(rule_id) as rule_key, \
                              course_id, \
                              offer_nbr, \
                              transfer_credits \
                      from destination_courses) to \
                      '`pwd`/rules_archive/${update_date}_destination_courses.csv' csv"
echo done

echo -n "effective_dates ... "
psql -Xqd cuny_curriculum -c "copy (select  rule_key(id) as rule_key, \
                              effective_date \
                      from transfer_rules) to \
                      '`pwd`/rules_archive/${update_date}_effective_dates.csv' csv"
echo done

echo Compressing
bzip2 `pwd`/rules_archive/*.csv

echo $SECONDS seconds
#! /usr/local/bin/bash

SECONDS=0

echo -n "transfer_rules ... "
psql -Xqd cuny_curriculum -c "copy (select  id, \
                              source_institution, \
                              destination_institution, \
                              subject_area, group_number \
                      from transfer_rules) to \
                      '`pwd`/rules_archive/`date -I`_transfer_rules.csv' csv"
echo done

echo -n "source_courses ... "
psql -Xqd cuny_curriculum -c "copy (select  rule_id, \
                              course_id, \
                              offer_nbr, \
                              min_credits, \
                              max_credits, \
                              credits_source, \
                              min_gpa, \
                              max_gpa \
                      from source_courses) to \
                      '`pwd`/rules_archive/`date -I`_source_courses.csv' csv"
echo done

echo -n "destination_courses ... "
psql -Xqd cuny_curriculum -c "copy (select  rule_id, \
                              course_id, \
                              offer_nbr, \
                              transfer_credits \
                      from destination_courses) to \
                      '`pwd`/rules_archive/`date -I`_destination_courses.csv' csv"
echo done

echo $SECONDS seconds
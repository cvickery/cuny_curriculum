#! /usr/local/bin/python3
""" Active courses and the term they were last offered.
    Not currently used, but but potentially useful for prioritizing rules than need to be updated.
"""
import csv
import os
import sys

from collections import namedtuple
from pgconnection import PgConnection

if __name__ == "__main__":
  conn = PgConnection()
  cursor = conn.cursor()
  with open('./latest_queries/qns_cv_class_max_term.csv') as csv_file:
    csv_reader = csv.reader(csv_file)
    for line in csv_reader:
      if 1 == csv_reader.line_num:
        Row = namedtuple('Row', ' '.join([c.lower().replace(' ', '_') for c in line]))
        cursor.execute("""
        drop table if exists class_max_term;
        create table class_max_term (
        institution text,
        max_term integer,
        course_id integer,
        offer_nbr integer,
        career text,
        class_status text,
        primary key (course_id, offer_nbr))
        """)
      else:
        row = Row._make(line)
        if row.academic_career != 'UGRD':
          continue
        cursor.execute(f"""
        insert into class_max_term values
        ('{row.institution}', {int(row.max_term)}, {int(row.course_id)}, {int(row.offer_nbr)},
         '{row.academic_career}', '{row.class_status}')
        """)
conn.commit()

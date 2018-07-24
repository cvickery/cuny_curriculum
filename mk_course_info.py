""" Create a table, course_info, of the records in the catalog query. For exploring what it means
    to be a course, and what it means to be a cross-listed course.
    NOTE: To convert the equiv_course_group from text to integer, do thw following after the fact:

    -- http://www.postgresonline.com/journal/archives/29-How-to-convert-a-table-column-to-another-data-type.html
      CREATE OR REPLACE FUNCTION pc_chartoint(chartoconvert character varying)
        RETURNS integer AS
      $BODY$
      SELECT CASE WHEN trim($1) SIMILAR TO '[0-9]+'
              THEN CAST(trim($1) AS integer)
          ELSE NULL END;

      $BODY$
        LANGUAGE 'sql' IMMUTABLE STRICT;

      ALTER TABLE course_info
        ALTER COLUMN equiv_course_group TYPE integer USING pc_chartoint(equiv_course_group);

    -- Then you could try this, but it fails because there are bogus values in equiv_course_group:
    -- Update 2018-06-26: This works now that the bogus values have been cleaned up in CUNYfirst.
    --                    Changed function name from pc_chartoint to text_to_integer.
        ALTER TABLE course_info ADD CONSTRAINT fk_equiv_course_group
        FOREIGN KEY(equiv_course_group) REFERENCES crse_equiv_tbl(equivalent_course_group);
"""

import sys
import csv
from collections import namedtuple

import psycopg2

conn = psycopg2.connect('dbname=vickery')
cursor = conn.cursor()

with open('./latest_queries/QNS_QCCV_CU_CATALOG_NP.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  raw = next(csv_reader)
  raw[0] = raw[0].replace('\ufeff', '')
  Row = namedtuple('Row', [val.lower().replace(' ', '_').replace('/', '_') for val in raw])
  create_query = """
drop table if exists course_info;
create table course_info (\n
"""
  for field in range(len(Row._fields)):
    create_query = create_query + f'  {Row._fields[field]} text,\n'
  create_query = create_query + 'primary key(course_id, offer_nbr))'
  print(create_query)
  cursor.execute(create_query)
  # prev_raw = next(csv_reader, False)
  prev_raw = raw
  raw = next(csv_reader, False)

  n = 1
  # populate the table
  while raw:
    # if raw == prev_raw:
    #   continue
    n = n + 1
    print(f'Row {n:,}\r', end='', file=sys.stderr)
    row = Row._make(raw)
    query = 'insert into course_info values(\n'
    for value in row:
      query = query + '%s,'
    query = query.strip(',') + ') on conflict do nothing'
    cursor.execute(query, [value for value in row])
    if cursor.rowcount != 1:
      print(f'\nIgnoring duplicate record(s) at row {n}: {raw}\n')
    prev_raw = raw
    raw = next(csv_reader, False)
print('', file=sys.stderr)
query = """
CREATE OR REPLACE FUNCTION text_to_integer(chartoconvert character varying)
  RETURNS integer AS
$BODY$
SELECT CASE WHEN trim($1) SIMILAR TO '[0-9]+'
        THEN CAST(trim($1) AS integer)
    ELSE NULL END;
$BODY$
  LANGUAGE 'sql' IMMUTABLE STRICT;

ALTER TABLE course_info
  ALTER COLUMN equiv_course_group TYPE integer USING text_to_integer(equiv_course_group);

ALTER TABLE course_info ADD CONSTRAINT fk_equiv_course_group
  FOREIGN KEY(equiv_course_group) REFERENCES crse_equiv_tbl(equivalent_course_group);
"""
cursor.execute(query)
cursor.close()
conn.commit()
conn.close()

""" Clear and re-populate the careers table.
"""

import psycopg2
import csv

db = psycopg2.connect('dbname=cuny_curriculum')
cur = db.cursor()
cur.execute('drop table if exists cuny_careers cascade')
cur.execute(
    """
    create table cuny_careers (
    institution text references institutions,
    career text,
    description text,
    is_graduate boolean,
    primary key (institution, career))
    """)
with open('./latest_queries/ACAD_CAREER_TBL.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols is None:
      row[0] = row[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      if row[cols.index('institution')] in ['UAPC1', 'MHC01']:
        continue
      is_graduate = 0
      if row[cols.index('graduate')] == 'Y':
        is_graduate = 1
      q = """insert into cuny_careers values('{}', '{}', '{}', cast({} as boolean))""".format(
          row[cols.index('institution')],
          row[cols.index('career')],
          row[cols.index('descr')],
          is_graduate)
      cur.execute(q)
  db.commit()
  db.close()

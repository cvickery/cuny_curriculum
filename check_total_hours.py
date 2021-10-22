# Generate a report showing active courses where the number of contact hours is not the
# sum of the component contact hours.

import psycopg
from psycopg.rows import namedtuple_row

from collections import namedtuple

Component = namedtuple('Component', 'component hours')
db = psycopg.connect('dbname=cuny_curriculum')
cursor = db.cursor(row_factory=namedtuple_row)

cursor.execute("""select  course_id,
                          institution,
                          discipline,
                          catalog_number,
                          course_status,
                          contact_hours,
                          components,
                          designation,
                          attributes
                    from  cuny_courses
                 order by course_status, institution, discipline, catalog_number""")
for row in cursor.fetchall():
  components = [Component._make(c) for c in row.components]
  hours = sum([component.hours for component in components])
  if hours != row.contact_hours and row.course_status == 'A':
    attributes = row.attributes
    if len(attributes) > 20:
      attributes = attributes[0:17] + '...'
    print('{:06} {} {:>6} {:<8} {} {:<4} {:<20} {:6.1f} {:6.1f} {}'
          .format(row.course_id,
                  row.institution,
                  row.discipline,
                  row.catalog_number,
                  row.course_status,
                  row.designation,
                  attributes,
                  row.contact_hours,
                  hours,
                  row.components))
exit()

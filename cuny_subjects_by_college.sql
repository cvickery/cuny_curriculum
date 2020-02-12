-- what colleges offer courses in what cuny subjects?
copy (
select cuny_subject,
       subject_name,
       string_agg(distinct institution, ', ') as colleges
from cuny_courses, cuny_subjects
where subject = cuny_subject
  and course_status = 'A'
  and can_schedule = 'Y'
  and attributes !~ 'BKCR'
group by cuny_subject, subject_name
order by cuny_subject, colleges) to '/Users/vickery/Desktop/cuny_subjects_by_college.csv' header csv

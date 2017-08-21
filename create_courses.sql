DROP TABLE IF EXISTS courses;

CREATE TABLE courses (
  course_id integer primary key,
  institution text references institutions,
  cuny_subject text references cuny_subjects,
  department text references cuny_departments,
  discipline text,
  number text,
  title text,
  hours text,
  credits text,
  requisites text,
  designation text references designations,
  description text,
  career text,
  course_status text,
  discipline_status text,
  can_schedule text,
  foreign key (institution, career) references cuny_careers
  )

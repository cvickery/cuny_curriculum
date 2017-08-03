DROP TABLE IF EXISTS courses;

CREATE TABLE courses (
  course_id number,
  institution text references institutions,
  cuny_subject text references cuny_subjects,
  department text references departments,
  discipline text,
  number text,
  title text,
  hours text,
  credits text,
  requisites text,
  designation text references designations,
  description text,
  career text references careers,
  course_status text,
  discipline_status text,
  can_schedule text,
  primary key (course_id, course_status))
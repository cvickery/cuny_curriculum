DROP TABLE IF EXISTS courses;

CREATE TABLE courses (
  course_id number,
  institution text references institutions,
  cuny_subject text references cuny_subjects,
  discipline text,
  number text,
  title text,
  hours text,
  credits text,
  requisites text,
  designation text references designations,
  description text,
  career text references careers,
  status text,
  primary key (course_id, status))
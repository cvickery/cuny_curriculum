DROP TABLE IF EXISTS courses;
CREATE TABLE courses (
  course_id number primary key,
  institution text,
  cuny_subject text,
  discipline text,
  number text,
  title text,
  hours text,
  credits text,
  requisites text,
  designation text,
  description text,
  career text,
  status text)
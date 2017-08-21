-- Create the course_attributes table
-- It will be populated by update_db.py
drop table if exists course_attributes;
create table course_attributes (
  course_id integer, -- references courses(course_id),
  institution text references institutions,
  name text,
  value text)
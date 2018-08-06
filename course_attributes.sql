-- Create the course_attributes table
-- It will be populated by update_db.py
drop table if exists course_attributes;
create table course_attributes (
  course_id integer,
  offer_nbr integer,
  institution text references institutions,
  name text,
  value text,
  primary key (course_id, offer_nbr, name, value),
  foreign key (course_id, offer_nbr) references courses)
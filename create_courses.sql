DROP TABLE IF EXISTS courses cascade;

CREATE TABLE courses (
  course_id integer primary key,
  institution text references institutions,
  cuny_subject text references cuny_subjects,
  department text references cuny_departments,
  discipline text,
  catalog_number text,
  title text,
  hours float,
  min_credits float,
  max_credits float,
  credits float,      -- academic progress units
  fa_credits float,   -- financial aid units
  requisites text,
  designation text references designations,
  description text,
  career text,
  course_status text,
  discipline_status text,
  can_schedule text,
  foreign key (institution, career) references cuny_careers
  )

DROP TABLE IF EXISTS courses cascade;

CREATE TABLE courses (
  course_id integer,
  offer_nbr integer,
  institution text references institutions,
  cuny_subject text references cuny_subjects,
  department text references cuny_departments,
  discipline text,
  catalog_number text,
  title text,
  components jsonb,  -- array of (component, hours, min_credits, max_credits)
  requisites text,
  designation text references designations,
  description text,
  career text,
  course_status text,
  discipline_status text,
  can_schedule text,
  primary key (course_id, offer_nbr),
  foreign key (institution, career) references cuny_careers
  )

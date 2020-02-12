DROP TABLE IF EXISTS cuny_courses cascade;
DROP TABLE IF EXISTS course_attributes cascade;

CREATE TABLE course_attributes (
  name text,
  value text,
  description text,
  primary key (name, value)
);

--  No course history, just the most recent version.
CREATE TABLE cuny_courses (
  course_id integer,
  offer_nbr integer,
  equivalence_group integer references crse_equiv_tbl,
  institution text references institutions,
  cuny_subject text references cuny_subjects,
  department text references cuny_departments,
  discipline text,
  catalog_number text,
  title text,
  short_title text,
  components jsonb,  -- array of [component, component_contact_hours]
  contact_hours real,
  min_credits real,
  max_credits real,
  primary_component text,
  requisites text,
  designation text references designations,
  description text,
  career text,
  course_status text,
  discipline_status text,
  can_schedule text,
  effective_date date,
  attributes text, -- semicolon-separated list of name:value pairs
  primary key (course_id, offer_nbr),
  foreign key (institution, career) references cuny_careers,
  foreign key (institution, discipline) references cuny_disciplines
)

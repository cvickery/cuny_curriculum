DROP TABLE IF EXISTS courses cascade;

CREATE TABLE courses (
  course_id integer,
  offer_nbr integer,
  equivalence_group integer references crse_equiv_tbl,
  institution text references institutions,
  cuny_subject text references cuny_subjects,
  department text references cuny_departments,
  discipline text,
  catalog_number text,
  title text,
  components jsonb,  -- array of [component, component_contact_hours]
  contact_hours float,
  min_credits float,
  max_credits float,
  primary_component text,
  requisites text,
  designation text references designations,
  description text,
  career text,
  course_status text,
  discipline_status text,
  can_schedule text,
  attributes text,  -- semicolon-separated list of course attribute descriptions
  primary key (course_id, offer_nbr),
  foreign key (institution, career) references cuny_careers,
  foreign key (institution, discipline) references disciplines
)

drop table if exists raw_rules;
create table raw_rules (
  source_institution text,
  source_course_id integer,
  source_offer_nbr integer,
  component_subject_area text,
  source_catalog_num text,
  src_equivalency_component text,
  equivalency_sequence_num integer,
  src_min_units real,
  src_max_units real,
  min_grade_pts real,
  max_grade_pts real,
  transfer_priority integer,
  source_career text,
  destination_institution text,
  destination_discipline text,
  destination_catalog_num text,
  destination_course_id integer,
  destination_offer_nbr integer,
  units_taken real,
  dest_min_units real,
  dest_max_units real,
  destination_career text,
  dest_equivalency_component text,
  internal_equiv_course_value_a text,
  internal_equiv_course_value_b text,
  contingent_credit text,
  input_course_count integer,
  transfer_subject_eff_date date,
  transfer_component_eff_date date,
  source_inst_eff_date date,
  transfer_to_eff_date date,
  crse_offer_eff_date date,
  crse_offer_view_eff_date date)

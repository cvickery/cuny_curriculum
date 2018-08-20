drop view if exists view_rules, view_sources, view_destinations;
create view view_rules as
(
  select  t.id as rule_id,
    t.source_institution || '-' ||
    t.destination_institution  || '-' ||
    t.subject_area || '-' ||
    t.group_number,
    t.source_disciplines
  from transfer_rules t
);

create view view_sources as
(
  select rule_id, string_agg(trim(to_char(course_id, '000000')), ':') as source_course_ids
  from source_courses
  group by rule_id
);

create view view_destinations as
(
  select rule_id, string_agg(trim(to_char(course_id, '000000')), ':') as destination_course_ids
  from destination_courses
  group by rule_id
);

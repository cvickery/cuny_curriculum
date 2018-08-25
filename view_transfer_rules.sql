drop view if exists view_transfer_rules, view_source_courses, view_destination_courses;
create view view_transfer_rules as
(
  select  t.id as rule_id,
    t.source_institution,
    t.destination_institution,
    t.subject_area,
    t.group_number,
    t.source_disciplines,
    t.review_status,
    t.source_institution || '-' ||
    t.destination_institution  || '-' ||
    t.subject_area || '-' ||
    t.group_number as rule_key
  from transfer_rules t
);

create view view_source_courses as
(
  select rule_id, string_agg(trim(to_char(course_id, '000000')), ':') as source_course_ids
  from source_courses
  group by rule_id
);

create view view_destination_courses as
(
  select rule_id, string_agg(trim(to_char(course_id, '000000')), ':') as destination_course_ids
  from destination_courses
  group by rule_id
);

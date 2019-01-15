drop view if exists view_transfer_rules, view_source_courses, view_destination_courses;
create view view_transfer_rules as
(
  select  t.id as rule_id,
          t.source_institution as from,
          t.destination_institution as to,
          t.subject_area as subj,
          t.group_number as group,
          t.source_disciplines as src_discps,
          t.review_status,
          t.source_institution || '-' ||
          t.destination_institution  || '-' ||
          t.subject_area || '-' ||
          t.group_number as rule_key,
          string_agg(trim(to_char(s.course_id, '000000')), ':') as src_crse_ids,
          string_agg(trim(to_char(d.course_id, '000000')), ':') as dst_crse_ids
     from transfer_rules t, source_courses s, destination_courses d
    where  s.rule_id = t.id
      and  d.rule_id = t.id
  group by  t.id,
            t.source_institution,
            t.destination_institution,
            t.subject_area,
            t.group_number,
            t.source_disciplines,
            t.review_status,
            t.source_institution,
            t.destination_institution,
            t.subject_area,
            t.group_number,
            s.rule_id,
            d.rule_id

);

create view view_source_courses as
(
  select rule_id as src_rule_id, string_agg(trim(to_char(course_id, '000000')), ':') as src_crse_ids
  from source_courses
  group by rule_id
);

create view view_destination_courses as
(
  select rule_id as dst_rule_id, string_agg(trim(to_char(course_id, '000000')), ':') as dst_crse_ids
  from destination_courses
  group by rule_id
);

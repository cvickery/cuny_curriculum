-- These views are no longer needed: the transfer_rules table now includes the rule_key and lists
-- of sending and receiving courses.
drop view if exists view_transfer_rules, view_source_courses, view_destination_courses;
create view view_transfer_rules as
(
  select  t.id as rule_id,
          rule_key(t.id),
          t.source_disciplines as sending_disciplines,
          string_agg(d.discipline, ':') as receiving_discipline,
          string_agg(trim(to_char(s.course_id, '000000'))||'.'||s.offer_nbr, ':') as sending_courses,
          string_agg(trim(to_char(d.course_id, '000000'))||'.'||d.offer_nbr, ':') as receiving_courses
     from transfer_rules t, source_courses s, destination_courses d
    where  s.rule_id = t.id
      and  d.rule_id = t.id
  group by  t.id,
            t.source_disciplines,
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

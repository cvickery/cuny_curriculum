-- Two versions -- the first doesn’t show all the courses
-- key, plan, subplan all have varying widths, so requirement_name is as wide as possible while
-- fitting each line in a 132 col window ... most of the time.
drop view if exists view_requirements;
create view view_requirements as
  select requirement_key    as key,
         institution,
         definition_block   as block_id,
         block_anomalies    as anomalies,
         plan,
         subplan,
         substr(name, 1,24) as requirement_name,
         hide_rule,
         requirement_description->'description' as descr,
         json_array_length(requirement_description->'courses'->'active_courses') as courses
    from dgw.requirements
;

drop view if exists view_requirement_courses;
create view view_requirement_courses as
  select requirement_key    as key,
         institution,
         definition_block   as block_id,
         block_anomalies    as anomalies,
         plan,
         subplan,
         substr(name, 1,24) as requirement_name,
         hide_rule,
         requirement_description->'description' as descr,
         requirement_description->'courses'->'active_courses' as courses
    from dgw.requirements
;

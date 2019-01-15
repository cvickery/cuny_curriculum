--  View fields of courses that will fit on one line.
--    Truncate title; omit description and attributes.
--    Used during development to look up courses quickly.
drop view if exists view_courses cascade;
create view view_courses as
(
  select  course_id as id,
          offer_nbr as offer,
          institution as inst,
          discipline as discp,
          catalog_number as cat_num,
          substr(title, 1, 25) as title,
          course_status as status,
          designation as desig,
          contact_hours as hr,
          min_credits||'-'||max_credits as cr
  from courses
)
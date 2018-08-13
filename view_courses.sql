--  View fields of courses that will fit on one line.
--    Truncate title; omit description and attributes.
--    Used during development to look up courses quickly.
drop view if exists view_courses cascade;
create view view_courses as
(
  select  course_id,
          offer_nbr,
          institution,
          discipline,
          catalog_number,
          substr(title, 1, 25) as title,
          course_status,
          designation
  from courses
)
--  View fields of courses that will fit on one line.
--    Truncate title; omit description and attributes.
--    Used during development to look up courses quickly.
drop view if exists view_courses cascade;
create view view_courses as
(
  select lpad(course_id::text, 6, '0') as id,
         offer_nbr as "#",
         institution as coll,
         discipline as discp,
         numeric_part(catalog_number) as cat_num,
         substr(title, 1, 40) as title,
         contact_hours as hr,
         min_credits||'-'||max_credits as cr,
         course_status as status,
         designation,
         attributes
    from cuny_courses
order by institution, discipline, cat_num);
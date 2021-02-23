--  View fields of courses that will fit on one line.
--    Truncate title; omit description and attributes.
--    Used during development to look up courses quickly.
drop view if exists view_courses cascade;
create view view_courses as (
    select institution,
           lpad(course_id::text, 6, '0') as course_id,
           offer_nbr,
           discipline,
           catalog_number,
           title,
           contact_hours,
           min_credits,
           max_credits,
           designation,
           attributes,
           course_status
    from cuny_courses
order by institution, discipline, catalog_number);

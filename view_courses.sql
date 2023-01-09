-- course and requirement block views

-- Shorten field names to make lines less wide
DROP VIEW IF EXISTS view_courses;
CREATE VIEW view_courses AS (
SELECT cuny_courses.institution,
    cuny_courses.course_id as id,
    cuny_courses.offer_nbr as nr,
    cuny_courses.discipline||' '||cuny_courses.catalog_number as course,
    substr(cuny_courses.title, 0, 32) ||
        CASE
            WHEN length(cuny_courses.title) > 32 THEN '...'::text
            ELSE ''::text
        END AS title,
    cuny_courses.contact_hours as hr,
    cuny_courses.min_credits as min,
    cuny_courses.max_credits as max,
    cuny_courses.designation as rd,
    cuny_courses.course_status as stat,
    substr(cuny_courses.attributes, 0, 16) ||
        CASE
            WHEN length(cuny_courses.attributes) > 16 THEN '...'::text
            ELSE ''::text
        END AS attr
   FROM cuny_courses
  ORDER BY cuny_courses.institution, cuny_courses.discipline, cuny_courses.catalog_number
);

DROP VIEW IF EXISTS view_blocks;
CREATE VIEW view_blocks AS (
SELECT institution,
       requirement_id,
       block_type,
       block_value,
       title,
       major1,
       period_stop,
       term_info is not null as is_active
  FROM requirement_blocks
);

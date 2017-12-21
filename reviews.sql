-- The pending_reviews table
drop table if exists pending_reviews;
create table pending_reviews (
token text primary key,
email text,
reviews text,
when_entered timestamptz default now()
);

-- The (review_)events table
drop table if exists events;

create table events (
id serial primary key,
source_institution text,
discipline text,
group_number integer,
destination_institution text,
event_type text references review_status_bits(abbr),
who text,
what text,
event_time timestamptz default now(),
foreign key (source_institution, discipline, group_number, destination_institution)
  references rule_groups
);

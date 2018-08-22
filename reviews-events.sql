-- The pending_reviews table
drop table if exists pending_reviews cascade;
create table pending_reviews (
token text primary key,
email text,
reviews text,
when_entered timestamptz default now()
);

-- The (review_)events table
drop table if exists events cascade;

create table events (
id serial primary key,
rule_id integer references transfer_rules,
event_type text references review_status_bits(abbr),
who text,
what text,
event_time timestamptz default now()
);

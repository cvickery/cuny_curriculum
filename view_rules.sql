-- Eliminate some cols from the transfer_rules table.
drop view if exists view_rules;
create view view_rules as
(
  select  to_char(id, '000000') as rule_id,
          rule_key,
          source_subjects,
          source_disciplines,
          destination_disciplines,
          sending_courses, receiving_courses, credit_sources,
          effective_date
     from transfer_rules
);

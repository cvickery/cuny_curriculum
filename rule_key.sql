-- Consturuct the rule key for a transfer rule, given its id.
CREATE OR REPLACE FUNCTION rule_key(rule_id int)
  RETURNS text AS
$$
  DECLARE
    result text;

  BEGIN
    select source_institution||':'||destination_institution||':'||replace(subject_area, ' ', '_')||':'||group_number
    into result from transfer_rules where id = rule_id;
    return result;
  END;
$$ LANGUAGE plpgsql;

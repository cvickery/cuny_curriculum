-- Use postgres to give numeric part of catalog numbers, divided down to less than 1000 if needed
CREATE OR REPLACE FUNCTION numeric_part(cat_num text) RETURNS real AS $$
DECLARE num real;

BEGIN
  num := (regexp_matches(cat_num, E'(\\d+\\.?\\d*)'))[1];
  while num > 1000.0 loop
  num := num / 10;
  end loop;
  return num;
END;
$$ LANGUAGE plpgsql;
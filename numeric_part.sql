-- Use postgres to give numeric part of catalog numbers, divided down to less than 1000 if needed
-- Returns -1 if there is no numeric part.
CREATE OR REPLACE FUNCTION numeric_part(cat_num text)
  RETURNS real AS
$$
  DECLARE
    matches text[];
    num real;

  BEGIN
    matches := regexp_matches(cat_num, E'(\\d+\\.?\\d*)');
    IF matches IS NOT NULL THEN
      num := matches[1]::REAL;
      WHILE num > 1000.0 LOOP
        num := num / 10;
      END LOOP;
      RETURN num;
    END IF;
    RETURN -1.0;
  END;
$$ LANGUAGE plpgsql;
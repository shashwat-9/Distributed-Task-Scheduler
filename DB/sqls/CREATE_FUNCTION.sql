-- The low and high values are for the range of the scheduled element. For e.g. week, 0-6, month 1-12
CREATE OR REPLACE FUNCTION get_schedule_resolved_for_cron_element(low INT, high INT, cron_exp TEXT)
RETURNS INT[]
LANGUAGE plpgsql
STRICT PARALLEL SAFE
AS $$
DECLARE
    scheduled_array INT[] := '{}';
    base_schedule TEXT;
    step INT := 1;
    i INT;
    item TEXT;
    lower_range INT;
    higher_range INT;
BEGIN
    -- Recursively handle comma-separated lists, e.g., "1,5,10-12"
    IF cron_exp LIKE '%,%' THEN
        FOREACH item IN ARRAY string_to_array(cron_exp, ',') LOOP
            scheduled_array := array_cat(scheduled_array, get_schedule_resolved_for_cron_element(low, high, item));
        END LOOP;
        RETURN array_sort_unique(scheduled_array);
    END IF;

    base_schedule := split_part(cron_exp, '/', 1);
    IF cron_exp LIKE '%/%' THEN
        step := split_part(cron_exp, '/', 2)::INTEGER;
    END IF;

    -- Handle ranges, e.g., "1-5" or "*"
    IF base_schedule = '*' THEN
        lower_range := low;
        higher_range := high;
    ELSIF base_schedule LIKE '%-%' THEN
        lower_range := split_part(base_schedule, '-', 1)::INTEGER;
        higher_range := split_part(base_schedule, '-', 2)::INTEGER;
    ELSE
        scheduled_array := scheduled_array || base_schedule::INTEGER;
        RETURN scheduled_array;
    END IF;

    i := lower_range;
    WHILE i <= higher_range LOOP
        scheduled_array := scheduled_array || i;
        i := i + step;
    END LOOP;

    RETURN scheduled_array;
END;
$$;

CREATE OR REPLACE FUNCTION get_next_scheduled_date(cron_exp TEXT, current_execution_date DATE)
RETURNS DATE
LANGUAGE plpgsql
STRICT PARALLEL SAFE
AS $$
DECLARE
    cron_parts TEXT[];
    scheduled_month INT[];
    scheduled_days INT[];
    scheduled_weekdays INT[];

    next_try_date DATE := current_execution_date;

    -- Limit the search to 5 years to prevent infinite loops on invalid cron strings
    max_date DATE := current_execution_date + INTERVAL '5 years';

BEGIN
    cron_parts := string_to_array(cron_exp, ' ');

    scheduled_month := get_schedule_resolved_for_cron_element(1, 12, cron_parts[4]);
    scheduled_days := get_schedule_resolved_for_cron_element(1, 31, cron_parts[3]);
    scheduled_weekdays := get_schedule_resolved_for_cron_element(0, 6, cron_parts[5]);

    LOOP
        next_try_date := next_try_date + INTERVAL '1 day';

        IF next_try_date > max_date THEN
            RAISE EXCEPTION 'Could not find a valid next schedule date within 5 years for cron: %', cron_exp;
        END IF;

        IF NOT (EXTRACT(MONTH FROM next_try_date) = ANY(scheduled_month)) THEN
            SELECT (date_trunc('month', next_try_date) + INTERVAL '1 month' - INTERVAL '1 day') INTO next_try_date;
            CONTINUE;
        END IF;

        IF (cron_parts[3] = '*') AND (cron_parts[5] = '*') THEN
            RETURN next_try_date;
        ELSIF (cron_parts[5] = '*') THEN
            IF (EXTRACT(DAY FROM next_try_date) = ANY(scheduled_days)) THEN
                RETURN next_try_date;
            END IF;
        ELSIF (cron_parts[3] = '*') THEN
            IF (EXTRACT(DOW FROM next_try_date) = ANY(scheduled_weekdays)) THEN
                RETURN next_try_date;
            END IF;
        ELSE
            IF (EXTRACT(DAY FROM next_try_date) = ANY(scheduled_days)) OR (EXTRACT(DOW FROM next_try_date) = ANY(scheduled_weekdays)) THEN
                RETURN next_try_date;
            END IF;
        END IF;

    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION array_sort_unique(arr anyarray)
RETURNS anyarray AS $$
BEGIN
    RETURN (
        SELECT array_agg(DISTINCT x ORDER BY x)
        FROM unnest(arr) AS x
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;
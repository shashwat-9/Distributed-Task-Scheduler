-- This procedure loads data into SCHEDULED_EXECUTION_TABLE every day, for the next day
CREATE OR REPLACE PROCEDURE insert_upcoming_scheduled_tasks_into_execution_table(batch_size INTEGER, offset_value INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    task_row SCHEDULED_TASKS%ROWTYPE;
    processed_in_batch BOOLEAN := FALSE;
BEGIN
    LOOP

        processed_in_batch := FALSE;
        FOR task_row IN SELECT * FROM SCHEDULED_TASKS WHERE NEXT_SCHEDULED_DATE = CURRENT_DATE + INTERVAL '1 day' AND CREATED_AT < CURRENT_DATE AND IS_ACTIVE = TRUE ORDER BY ID LIMIT batch_size OFFSET offset_value FOR SHARE
        LOOP
            processed_in_batch := TRUE;

            SAVEPOINT per_task_savepoint;
            BEGIN
                CALL insert_row_into_execution_table(task_row, CURRENT_DATE + INTERVAL '1 day');
                UPDATE SCHEDULED_TASKS SET NEXT_SCHEDULED_DATE = (SELECT find_the_next_scheduled_date(task_row.CRON_SCHEDULE, CURRENT_DATE + INTERVAL '1 day')) WHERE SCHEDULED_TASKS.ID = task_row.ID;
            EXCEPTION
                WHEN OTHERS THEN
                    ROLLBACK TO SAVEPOINT per_task_savepoint;
                    RAISE NOTICE 'The record with id % is unprocessed: ERR: %', task_row.ID, SQLERRM;
            END;


        END LOOP;

        EXIT WHEN NOT processed_in_batch;

        COMMIT;
        offset_value := offset_value + batch_size;
    END LOOP;
END;
$$;

-- Called by another transaction and thus no COMMIT/ROLLBACK inside this
CREATE OR REPLACE PROCEDURE insert_row_into_execution_table(task_row SCHEDULED_TASKS%ROWTYPE, execution_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    time_of_execution TIME;
BEGIN
    FOR time_of_execution IN SELECT find_the_scheduled_time(task_row.CRON_SCHEDULE)
    LOOP
        BEGIN
            INSERT INTO SCHEDULED_TASKS_EXECUTION(TASK, SCHEDULED_TIME, EXECUTION_DATE, SCHEDULED_TASK_ID) VALUES (task_row.TASK, time_of_execution, execution_date, task_row.ID);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE 'Record insertion failed in SCHEDULED_TASKS_EXECUTION Table with time_of_execution as % and execution_date % for record id %', time_of_execution, execution_date, task_row.ID;
                RAISE EXCEPTION 'Error [%]: %', SQLSTATE, SQLERRM;
        END;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION find_the_scheduled_time(cron_expr TEXT)
RETURNS TIME[]
LANGUAGE plpgsql
AS $$
DECLARE
    result_array TIME[] := '{}';
    cron_elements TEXT[];
    hour_list TEXT[];
    mins_list TEXT[];
    hour_val TEXT;
    minute_val TEXT;
    time TIME;
BEGIN
    cron_elements := string_to_array(cron_expr, ' ');
    SELECT find_the_collection_for_schedule_element(cron_elements, 1) INTO mins_list;
    SELECT find_the_collection_for_schedule_element(cron_elements, 2) INTO hour_list;

    FOREACH hour_val IN ARRAY hour_list
    LOOP
        FOREACH minute_val IN ARRAY mins_list
        LOOP
            time := (hour_val || ':' || minute_val || ':00')::TIME;
            result_array := array_append(result_array, time);
        END LOOP;
    END LOOP;

    RETURN result_array;
END;
$$;

CREATE OR REPLACE FUNCTION find_the_collection_for_schedule_element(cron_elements TEXT[], idx INTEGER)
RETURNS TEXT[]
LANGUAGE plpgsql
AS $$
DECLARE
    max_value_of_each_element INT[] := '{60, 24, 31, 12, 7}';
BEGIN

    -- Resolving elements at the idx position in cron
    IF cron_elements[idx] ~ '\\*/[1-n]{1}$' THEN

    ELSEIF cron_elements[idx] ~ '^[0-9]-[0-9]$' THEN

    ELSEIF cron_elements[idx] ~ '[0-9]' THEN

    ELSEIF cron_elements[idx] ~ '[0-9]' THEN

    END IF;

END;
$$;

CREATE OR REPLACE FUNCTION find_the_next_scheduled_date(cron_expr TEXT, execution_date DATE)
RETURNS DATE
LANGUAGE plpgsql
AS $$
DECLARE

BEGIN

END;
$$;
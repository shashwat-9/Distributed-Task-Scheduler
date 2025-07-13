CREATE OR REPLACE PROCEDURE insert_upcoming_scheduled_tasks_into_execution_table(batch_size INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    task_rec RECORD;
    processed_count INTEGER := 0;
    cur REFCURSOR;
    today DATE := CURRENT_DATE;
    fetched_count INTEGER := 0;
BEGIN
    OPEN cur FOR SELECT * FROM SCHEDULED_TASKS WHERE START_DATE <= today AND IS_ACTIVE = TRUE AND CREATED_AT < today ORDER BY ID;
    LOOP
        fetched_count = 0;
        LOOP
            FETCH cur INTO task_rec;
            EXIT WHEN NOT FOUND;
            CALL insert_row_into_execution_table(task_rec);
            fetched_count := fetched_count + 1;
        END LOOP;
        EXIT WHEN fetched_count = 0;
    END LOOP;

    CLOSE CUR;
    RAISE NOTICE 'EXECUTION_TABLE updated with new entries';
END;
$$;


CREATE OR REPLACE PROCEDURE insert_row_into_execution_table(task_row RECORD)
LANGUAGE plpgsql
AS $$
DECLARE
    cron_schedule_text TEXT;
    cron_array TEXT[];
BEGIN
    cron_schedule_text = task_row.CRON_SCHEDULE;
    cron_array := string_to_array(cron_schedule_text, ' ');
    cron_array[0];
END;
$$

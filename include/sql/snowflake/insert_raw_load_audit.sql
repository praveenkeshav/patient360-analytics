INSERT INTO PATIENT360_PROD.EHR_RAW.RAW_LOAD_AUDIT (
    TABLE_NAME,
    SOURCE_FILE,
    ROWS_LOADED,
    LOAD_STATUS,
    DAG_ID,
    DAG_RUN_ID,
    TASK_ID
)
SELECT
    '{{ params.table_name }}',
    '{{ params.source_file }}',
    COUNT(*),
    'SUCCESS',
    '{{ dag.dag_id }}',
    '{{ run_id }}',
    '{{ task.task_id }}'
FROM {{ params.raw_table }};
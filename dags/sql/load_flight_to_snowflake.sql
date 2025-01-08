COPY INTO "TEAM5"."RAW_DATA"."FLIGHT_DATA"
    FROM (
    SELECT $1:extracted_at:: VARCHAR, $1:departure_date:: VARCHAR, $1:departure_display_code:: VARCHAR, $1:departure_name:: VARCHAR, $1:arrival_display_code:: VARCHAR, $1:arrival_name:: VARCHAR, $1:carrier_names::VARIANT, $1:departure_time:: VARCHAR, $1:arrival_time:: VARCHAR, $1:agent_name:: VARCHAR, $1:amount:: FLOAT, $1:url::VARIANT, $1:last_updated:: VARCHAR, $1:stop_count:: VARCHAR
    FROM '@"TEAM5"."RAW_DATA"."TEAM5_STAGE"'
    )
    FILES = ('{files_list}')
    FILE_FORMAT = (
    TYPE =PARQUET,
    REPLACE_INVALID_CHARACTERS= TRUE,
    BINARY_AS_TEXT= FALSE
    )
    ON_ERROR=ABORT_STATEMENT;


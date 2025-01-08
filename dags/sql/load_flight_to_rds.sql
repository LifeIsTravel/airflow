COPY your_table_name
    FROM 's3://team5-s3/{s3_key}'
    IAM_ROLE '{iam_role_arn}'
    FORMAT AS PARQUET;
SELECT aws_s3.table_import_from_s3(
               'flight',
               'extracted_at, departure_date, departure_display_code, departure_name, arrival_display_code, arrival_name, carrier_names, departure_time, arrival_time, agent_name, amount, url, last_updated, stop_count',
               '(format csv, HEADER)',
               aws_commons.create_s3_uri(
                       'team5-s3',
                       '{s3_key}',
                       'ap-northeast-2'
               ));
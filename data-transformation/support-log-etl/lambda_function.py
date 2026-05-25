import boto3
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

def read_from_s3(bucket, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    log_data = response['Body'].read().decode('utf-8')
    return log_data

def save_parquet_to_s3(df, bucket, key):
    table = pa.Table.from_pandas(df, preserve_index=False)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)

    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())
    print(f"==> Parquet saved to s3://{bucket}/{key}")

def lambda_handler(event, context):
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    raw_data = read_from_s3(bucket, key)

    print(f"==> Triggered by: s3://{bucket}/{key}")

    entries = [entry.strip() for entry in raw_data.split("---") if entry.strip()]

    log_pattern = re.compile(
        r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(?P<log_level>[A-Za-z0-9_]+)\] '
        r'(?P<component>[^\s]+) - TicketID=(?P<ticket_id>[^\s]+) SessionID=(?P<session_id>[^\s]+)\s*'
        r'IP=(?P<ip>.*?) \| ResponseTime=(?P<response_time>-?\d+)ms \| CPU=(?P<cpu>[\d.]+)% \| EventType=(?P<event_type>.*?) \| Error=(?P<error>\w+)\s*'
        r'UserAgent="(?P<user_agent>.*?)"\s*'
        r'Message="(?P<message>.*?)"\s*'
        r'Debug="(?P<debug>.*?)"\s*'
        r'TraceID=(?P<trace_id>.*)'
    )

    parsed_entries = []
    for entry in entries:
        match = log_pattern.search(entry)
        if match:
            parsed_entries.append(match.groupdict())

    df = pd.DataFrame(parsed_entries)

    df = df.drop('trace_id', axis=1)
    df = df[df['response_time'].astype(int) >= 0]

    fix_log_level = {'INF0': 'INFO', 'DEBG': 'DEBUG', 'warnING': 'WARNING', 'EROR': 'ERROR'}
    df['log_level'] = df['log_level'].replace(fix_log_level)

    df = df.drop_duplicates()

    df['response_time'] = df['response_time'].astype(int)
    df['cpu'] = df['cpu'].astype(float)
    df['error'] = df['error'].str.lower().map({'true': True, 'false': False})

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce').astype('datetime64[ms]')

    output_file_name = key.split('/')[2].replace('.log', '.parquet')
    output_key = f'support-logs/processed/{output_file_name}'
    save_parquet_to_s3(df, bucket, output_key)
import boto3

glue = boto3.client('glue')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    s3_input_path = 's3://{bucket}/{key}'

    print(f"==> Triggering Glue job with file: {s3_input_path}")

    glue.start_job_run(
        JobName = 'support_tickets_ETL_auto',
        Arguments = {
            '--input_file_path': s3_input_path
        }
    )
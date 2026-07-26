import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):

    response = glue.start_job_run(
        JobName="EcommerceSalesTransformationJob"
    )

    return {
        "statusCode": 200,
        "message": "Transformation Job Started",
        "JobRunId": response["JobRunId"]
    }

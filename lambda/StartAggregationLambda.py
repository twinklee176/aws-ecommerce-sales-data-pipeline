import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):

    response = glue.start_job_run(
        JobName="EcommerceSalesAggregationJob"
    )

    return {
        "statusCode": 200,
        "message": "Aggregation Job Started",
        "JobRunId": response["JobRunId"]
    }

import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):

    glue.start_crawler(
        Name="EcommerceSalesCrawler"
    )

    return {
        "statusCode": 200,
        "message": "Crawler Started"
    }

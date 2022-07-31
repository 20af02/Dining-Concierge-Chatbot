import json
import datetime
import boto3
from variables import *
from boto3.dynamodb.conditions import Key


def get_prev_suggestion():
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1',
                              aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    table = dynamodb.Table('hw5_prev_state')

    response = table.query(KeyConditionExpression=Key('id').eq("1"))

    if len(response['Items']) == 0:
        return ""

    resp_str = response['Items'][0]['suggestions'].strip()
    return resp_str


class EST(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=-4)

    def dst(self, dt):
        return datetime.timedelta(0)


def lambda_handler(event, context):
    dsi_init_resp = "Great. I can help you with that. What city or city area are you looking to dine in?"

    ctime = datetime.datetime.now(EST())
    date_str = str(ctime.hour)+':'+str(ctime.minute)

    message = event["messages"][0]["unstructured"]["text"]

    lex_tags_client = boto3.client('lex-runtime', region_name='us-east-1',
                                   aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    response = lex_tags_client.post_text(
        botName=BOT_NAME,
        botAlias=BOT_ALIAS,
        userId=USER_ID,
        inputText=message)

    if response["message"] == dsi_init_resp:
        prev_suggestion = get_prev_suggestion()
        response["message"] = dsi_init_resp+"\n"+prev_suggestion

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type, Origin, X-Auth-Token',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': {
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "id": "1",
                        "text": response["message"],
                        "timestamp": date_str
                    }
                }
            ]

        }

    }

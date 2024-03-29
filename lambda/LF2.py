import boto3
import json
import requests
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key
from variables import *
from botocore.exceptions import ClientError


region = 'us-east-1' 
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
lexbot = boto3.client('lex-runtime')
sns = boto3.client('sns', region_name='us-east-1')


host = 'https://search-restaurants-bqoyr5brzwpidhcw2hmff3vkai.us-east-1.es.amazonaws.com'
index = 'restaurants'
url = host + '/' + index + '/_search'

def pull_sqs():
    try:
        sqs_client = boto3.client('sqs', region_name='us-east-1')
    except Exception as e:
        print(e)
    
    queue_url = 'https://sqs.us-east-1.amazonaws.com/219472459747/Q1'
    
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=1,
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
        )
    print('queue response is', response)
    try:
        message = response['Messages'][0]
    except:
        return
    # Delete the message
    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
    return message
    
def search_es_dynamodb(term):
    print("search term is ", term)
    query = {
        "size": 3,
        "query": {
            "multi_match": {
                "query": term,
                "fields": ["cuisine"]
            }
        }
    }
    
    headers = { "Content-Type": "application/json" }

    # Make the signed HTTP request
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))
    
    # # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
        },
        "isBase64Encoded": False
    }
    print("r.text", r.text)
    restaurants_list = json.loads(r.text)['hits']['hits']
    restaurants_id_list = [x['_id'] for x in restaurants_list]
    
    # get the posts details from dynamoDB
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('restaurants')
    
    restaurant_info = []
    for id in restaurants_id_list:
        resp = table.query(KeyConditionExpression=Key('business_id').eq(id))
        #print(resp)
        if len(resp['Items']) == 0:
            continue
        else:
            restaurant_info.append(resp['Items'][0])
            
    response['body'] = json.dumps(restaurant_info)
    print(restaurant_info)
    
    return restaurant_info


def send_email(recepient, message):
    SENDER = 'jc11224@nyu.edu'
    RECIPIENT = recepient
    AWS_REGION = "us-east-1"
    SUBJECT = 'Restaurant recommendation'
    
    BODY_TEXT = (message)
    CHARSET = "UTF-8"
    client = boto3.client('ses',region_name=AWS_REGION)
    
    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
            'Body': {
                'Text': {
                    'Charset': CHARSET,
                    'Data': BODY_TEXT,
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data': SUBJECT,
            },
        },
            Source=SENDER
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
    

def lambda_handler(event, context):
    
    
    queue = pull_sqs()
    if queue is None:
        print('No queue found')
        return
    print(queue)
    location = queue['MessageAttributes']['location']['StringValue']
    cuisine = queue['MessageAttributes']['Cuisine']['StringValue']
    number_of_people = queue['MessageAttributes']['NumberOfPeople']['StringValue']
    dining_date = queue['MessageAttributes']['DiningDate']['StringValue']
    dining_time = queue['MessageAttributes']['DiningTime']['StringValue']
    email = queue['MessageAttributes']['email']['StringValue']
    
    response = search_es_dynamodb(cuisine)
    print(response)
    restaurant_recom = []
    reminder_message = """
            Hello! Here are my {cuisine} restaurant suggestions for {number_of_people} people,
            for {dining_date} at {dining_time}: 
            """.format(cuisine=cuisine, number_of_people=number_of_people, dining_date=dining_date,
                dining_time=dining_time)
    rest_list_obs = """
    {}. {}, located at {}, """
    previous_state_obj=""
                
    for i in range(len(response)):
        business_id = response[i]['business_id']
        rating = response[i]['rating']
        zip_code = response[i]['zip_code']
        address = response[i]['address'].replace('[', '').replace(']', '').replace("'", '')
        restaurant_name = response[i]['name']
        reminder_message+= rest_list_obs.format(i+1, restaurant_name, address)
        previous_state_obj+=rest_list_obs.format(i+1, restaurant_name, address)
    reminder_message=reminder_message[:-2]
    reminder_message+= " \nEnjoy your meal!"
        
    
        
    print(reminder_message)
    
    #SEND MESSAGES

    send_email(email, reminder_message)
    
    # send previosu recommendation to dynanoDB
    
    prev_recommendation = """Based on your previous search for {cuisine} cuisine in {location}, you may be interested in the following restaurants:\n 
    """.format(cuisine=cuisine, location=location)
    
    prev_recommendation+=previous_state_obj
    prev_recommendation=prev_recommendation[:-2]
    #out_message = json.dumps({"1": prev_recommendation})
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1',
                                  aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    table = dynamodb.Table('hw5_prev_state')
    id="1" # whatever ID we use to store the state
    try:
        response = table.delete_item(
            Key={
            'id': '1'
            }
        )
        resp_obj={
                'id':'1',
                'suggestions': prev_recommendation
            
        }
        table.put_item(Item=resp_obj)
    except:
        print('dynamoDB insert error')
        
    print(prev_recommendation)
    

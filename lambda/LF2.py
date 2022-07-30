import boto3
import json
import requests
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key


region = 'us-east-1' 
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
lexbot = boto3.client('lex-runtime')
sns = boto3.client('sns', region_name='us-east-1')

#host = 'https://search-posts-olyfhy4stfviygw2xf35znnvmi.us-east-1.es.amazonaws.com'
host = 'https://search-restaurants-bqoyr5brzwpidhcw2hmff3vkai.us-east-1.es.amazonaws.com'
index = 'restaurants'
url = host + '/' + index + '/_search'

def pull_sqs():
    try:
        sqs_client = boto3.client('sqs', region_name='us-east-1')
    except Exception as e:
        print(e)
    
    queue_url = 'https://sqs.us-east-1.amazonaws.com/219472459747/Q1'
    #queue_url = 'https://sqs.us-east-1.amazonaws.com/051792343076/restaurants_recommendation_queue'
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
        "size": 1,
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

def lambda_handler(event, context):
    
    
    queue = pull_sqs()
    print(queue)
    location = queue['MessageAttributes']['location']['StringValue']
    cuisine = queue['MessageAttributes']['Cuisine']['StringValue']
    number_of_people = queue['MessageAttributes']['NumberOfPeople']['StringValue']
    dining_date = queue['MessageAttributes']['DiningDate']['StringValue']
    dining_time = queue['MessageAttributes']['DiningTime']['StringValue']
    email = queue['MessageAttributes']['Email']['StringValue']
    
    response = search_es_dynamodb(cuisine)
    print(response)
    business_id = response[0]['business_id']
    rating = response[0]['rating']
    zip_code = response[0]['zip_code']
    address = response[0]['address'].replace('[', '').replace(']', '').replace("'", '')
    restaurant_name = response[0]['name']
    
    reminder_message = """
        Hello! Here are my {cuisine} restaurant suggestions for {number_of_people} people,
        for {dining_date} at {dining_time}, {address}, {restaurant_name}. Enjoy your meal!
    """.format(cuisine=cuisine, number_of_people=number_of_people, dining_date=dining_date,
                dining_time=dining_time, address=address, restaurant_name=restaurant_name)
    print(reminder_message)
    
    #SEND MESSAGES
    topic_arn = 'arn:aws:sns:us-east-1:051792343076:chat_response_posts'
    # list subscription
    # subscription_list = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
    # print(subscription_list)
    # email_sub = sns.subscribe(TopicArn=topic_arn,
    #                       Protocol='email',
    #                       Endpoint="{email}".format(email=email))
    sns.publish(TopicArn=topic_arn,
            Message=reminder_message
                                    )
    #return response

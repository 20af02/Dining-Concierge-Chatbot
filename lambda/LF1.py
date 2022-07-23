import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import json
from variables import *

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


""" --- SQS Functionality --- """


def sendSQS(request_data):
    sqs_client = boto3.client('sqs')

    location = request_data["location"]
    cuisine = request_data["Cuisine"]
    number_of_people = request_data["NumberOfPeople"]
    dining_date = request_data["DiningDate"]
    dining_time = request_data["DiningTime"]
    phone_number = request_data["Phonenumber"]

    message_attributes = {
        "location": {
            'DataType': 'String',
            'StringValue': location
        },
        "Cuisine": {
            'DataType': 'String',
            'StringValue': cuisine
        },
        "NumberOfPeople": {
            'DataType': 'Number',
            'StringValue': number_of_people
        },
        "DiningDate": {
            'DataType': 'String',
            'StringValue': dining_date
        },
        "DiningTime": {
            'DataType': 'String',
            'StringValue': dining_time
        },
        "Phonenumber": {
            'DataType': 'Number',
            'StringValue': phone_number
        }
    }
    body = ('Resturant slots')

    response = sqs_client.send_message(
        QueueUrl=SQS_URL,  MessageAttributes=message_attributes, MessageBody=body)

    return


""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


""" --- Helper Functions --- """


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def validate_dining_suggestions(location, cuisine, number_of_people, dining_date, dining_time, phone_number):

    # Locations

    locations = ['manhattan']
    cuisines = ['italian', 'japanese']
    if location is not None and location.lower() not in locations:
        return build_validation_result(False,
                                       'location',
                                       'We do not have dining suggestions for {}, would you like suggestions for other locations?  '
                                       'Our most popular location is Manhattan'.format(location))

    # Cuisine
    if cuisine is not None and cuisine.lower() not in cuisines:
        return build_validation_result(False, 'Cuisine',
                                       'We do not have suggestions for {}, would you like suggestions for another cuisine?'
                                       'Our most popular cuisine is Italian'.format(cuisine))

    # Number of people
    if number_of_people is not None:
        number_of_people = parse_int(number_of_people)
        if not 0 < number_of_people < 30:
            return build_validation_result(False, 'NumberOfPeople', '{} does not look like a valid number, '
                                           'please enter a number less than 30'.format(number_of_people))

    # DiningDate
    if dining_date is not None:
        if not isvalid_date(dining_date):
            return build_validation_result(False, 'DiningDate', 'I did not understand that, what date would you like for your suggestion?')
        elif datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'DiningDate', 'You can pick a date from today onwards.  What day would you like for your suggestion?')

    # DiningTime
    if dining_time is not None:
        if len(dining_time) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'DiningTime', None)

        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'DiningTime', None)

        # Edge case
        ctime = datetime.datetime.now()

        if datetime.datetime.strptime(dining_date, "%Y-%m-%d").date() == datetime.datetime.today():
            if ctime.hour >= hour and ctime.minute > minute:
                return build_validation_result(False, 'DiningTime', 'Please select a time in the future.')

    # Phonenumber
    if phone_number is not None and not phone_number.isnumeric():
        if len(phone_number) != 10:
            return build_validation_result(False, 'Phonenumber', '{} is not a valid phone number,'
                                           'please enter a valid phone number'.format(phone_number))

    return build_validation_result(True, None, None)


""" --- Functions that control the bot's behavior --- """


def dining_suggestions(intent_request):
    """
    Performs dialog management and fulfillment for dining suggestions.
    Beyond fulfillment, the implementation of this intent demonstrates the use of the elicitSlot dialog action
    in slot validation and re-prompting.
    """
    slots = get_slots(intent_request)
    location = slots["location"]
    cuisine = slots["Cuisine"]
    number_of_people = slots["NumberOfPeople"]
    dining_date = slots["DiningDate"]
    dining_time = slots["DiningTime"]
    phone_number = slots["Phonenumber"]
    source = intent_request['invocationSource']

    request_data = {
        "location": location,
        "Cuisine": cuisine,
        "NumberOfPeople": number_of_people,
        "DiningDate": dining_date,
        "DiningTime": dining_time,
        "Phonenumber": phone_number
    }
    output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {
    }
    output_session_attributes['requestData'] = json.dumps(request_data)

    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        # Use the elicitSlot dialog action to re-prompt for the first violation detected.
        slots = get_slots(intent_request)

        validation_result = validate_dining_suggestions(
            location, cuisine, number_of_people, dining_date, dining_time, phone_number)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {
        }

        return delegate(output_session_attributes, get_slots(intent_request))

    sendSQS(slots)

    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': ' You’re all set. Expect my suggestions shortly! Have a good day.'})


""" --- Intents --- """


def greeting_intent(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Hi there, how can I help?'})


def thank_you_intent(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': "You're welcome."})


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(
        intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DinningSuggestionsIntent':
        return dining_suggestions(intent_request)
    elif intent_name == 'GreetingIntent':
        return greeting_intent(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    # logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)

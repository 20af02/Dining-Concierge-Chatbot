# Dining-Concierge-Chatbot
CS-GY 9223

## Overview:

A serverless, microservice-driven Dining Concierge chatbot that sends you restaurant suggestions, given a set of preferences that you provide through conversation.

## Architecture:

<img width="1000" alt="Architecture diagram (1)" src="https://github.com/20af02/Dining-Concierge-Chatbot/blob/main/screenshots/architecture-diagram.jpg">


## Sample Bot Interaction

<img width="1000" alt="Sample Interaction (2)" src="https://github.com/20af02/Dining-Concierge-Chatbot/blob/main/screenshots/example-interaction.jpg">

## Implementation: 

The following points describe how the project has been implemented:
1.	The frontend is written in JavaScript and is hosted on AWS S3 bucket.
2.	The API for the application is set up through AWS API Gateway.
3.	A Lambda function (LF0) performs the chat operation using the request/response model (interfaces) specified in the API specification. When the API receives a request, it: a) Extracts the text message from the API request, b) Sends it to the Lex chatbot, c) Waits for the response, d) Sends back the response from Lex as the API response.
4.	The Dining Concierge chatbot is built using Amazon Lex with the following functionalities: a) GreetingIntent b) ThankYouIntent c) DiningSuggestionsIntent
5.	For the DiningSuggestionsIntent, the following info is collected from the user: a) Location b) Cuisine c) Dining Time d) Number of people e) Phone number/Email ID. Based on the parameters collected from the user (location, cuisine, etc.) this info is pushed to an SQS queue (Q1).
6.	The Lambda function (LF1) acts as a code hook for Lex, which essentially entails the invocation of the Lambda before Lex responds to any of the requests, allowing for the manipulation and validation of parameters, formatting the bot’s responses, and notifying the user that their request has been received. Notifications are sent over SMS/Email once the list of restaurant suggestions is ready.
7.	The data for different restaurants (Business ID, Name, Address, Coordinates, Number of Reviews, Rating, Zip Code) is collected using the Yelp API and is stored in a DynamoDB table named “yelp-restaurants”.
8.	Using the AWS ElasticSearch Service, an index “restaurants” is created to store partial information (RestaurantID and Cuisine) for each restaurant.
9.	A third Lambda function (LF2) (a suggestions module decoupled from the Lex chatbot) acts as a queue worker. Whenever it is invoked, it a) Pulls a message from the SQS queue (Q1), b) Gets a random restaurant recommendation for the cuisine collected through conversation from ElasticSearch and DynamoDB, c) Formats them d) Sends them over text message to the phone number included in the SQS message, using SNS/SES
10.	Lastly, a CloudWatch event trigger is created that runs every minute and invokes the Lambda function automating the queue worker Lambda to poll and process suggestion requests on its own.

In summary, the LEX chatbot will identify the customer’s preferred ‘cuisine’ based on a conversation with the customer. It will search through ElasticSearch to get random suggestions of restaurant IDs with this cuisine. At this point, it will also query the DynamoDB table with these restaurant IDs to find more information about the restaurants to suggest to the customers, like the name and address of the restaurant. Additionally, there is a state functionality that remembers the last search for both location and category. When users return to chat, they automatically receive a recommendation based on a previous search, where intermediary state data is stored in DynamoDB.

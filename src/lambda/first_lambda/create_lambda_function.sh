#!/bin/bash

# Variables
STACK_NAME="oneders-stack4"
TEMPLATE_FILE="create-lambda.yaml"

# Validate the CloudFormation template
aws cloudformation validate-template --template-body file://$TEMPLATE_FILE --region eu-west-1 --profile generation-delon11

# Create the CloudFormation stack
aws cloudformation create-stack --stack-name $STACK_NAME --template-body file://$TEMPLATE_FILE --capabilities CAPABILITY_NAMED_IAM --region eu-west-1 --profile generation-delon11


#Saad: I made a change to line 11, where i changed capabilites from CAPABILITY_IAM to CAPABILITY_NAMED_IAM
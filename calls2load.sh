#!/bin/bash
json={"\"bucketname\"":"\"termproject-testing\"","\"filename\"":"\"result.csv\""}

#echo " "
echo "Invoking Load Service as a Lambda Function using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name S2_Load --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo $output | jq



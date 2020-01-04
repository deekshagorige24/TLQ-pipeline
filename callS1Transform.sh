#!/bin/bash
json={"\"bucketname\"":"\"termproject-testing\"","\"filename\"":"\"testdataset.csv\""}

#echo " "
echo "Invoking Extract and Transform Services as a lambda function using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name S1_Transform --region us-east-1 --payload $json /dev/stdout; echo`
echo $output | jq

#!/bin/bash

# JSON object to pass to Lambda Function
json={"\"bucketName\"":"\"tcss562.mylogs.ali\",""\"objectKey\"":"\"loaded/sale.db\""}
echo $json
#echo "Invoking Lambda function using API Gateway"
output=`curl -s -H "Content-Type: application/json" -X POST -d  $json {https://ppikta5swe.execute-api.us-east-1.amazonaws.com/CreateCSV}`
#
#echo ""
#echo "CURL RESULT:"
#echo $output
#echo ""
#echo ""
#time
echo "Invoking Lambda function using AWS CLI"
output=`aws lambda invoke --invocation-type RequestResponse --function-name Extraction_Part --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c 200000 ; echo`
echo ""
echo "AWS CLI RESULT:"
echo $output
echo ""








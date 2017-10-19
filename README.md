# Serverless

Collection of code samples to do a bunch of stuff with lambda and other AWS services

- [BatchWriteToDDBwithLambda.js](/DataProcessing/BatchWriteToDDBwithLambda.js): Takes a file with lots of JSON elements in an S3 bucket and performs `batchWrite()` to a table in DynamoDB.  The logic for DynamoDB.batchWrite() is funky, make sure you read the [documentation](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html) because you may get errors for unprocessed data and for throttling. Also remember to setup the correct trigger in lambda for S3 and use the right IAM roles. More details [here](https://github.com/awslabs/aws-serverless-workshops/tree/master/DataProcessing/1_FileProcessing)

- [WriteToDDBwithLambda.js](/DataProcessing/WriteToDDBwithLambda.js): // simple and straightforward function to write to DynamoDB a single  object `dynamoItem` using `AWS.DynamoDB.DocumentClient()`. DocumentClient  annotates native JavaScript types supplied as input parameters, as well as converts annotated response data to native JavaScript types.


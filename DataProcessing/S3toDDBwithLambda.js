'use strict';

const AWS = require('aws-sdk');

const S3 = new AWS.S3();
const DynamoDB = new AWS.DynamoDB.DocumentClient();

// make sure you add an environment name to lambda
const TABLE_NAME = process.env.TABLE_NAME;
const THROTTLING_ERRORS = [
    'ProvisionedThroughputExceededException',
    'ThrottlingException',
];

exports.handler = (event, context, callback) => {

    // create an array of promises to upload
    // a bunch of JSON records in S3 to DynamoDB
    const promises = event.Records.map((record) => {
        const params = {
            Bucket: record.s3.bucket.name,
            Key: record.s3.object.key,
        };
        // this is the meat and potato of the fuction:
        // read objects in S3, format the request, write it to DynamoDB
        return S3.getObject(params).promise()
            .then(data => buildRequests(data.Body))
            .then(items => processRequests(items));
    });
    // takes an array of promises and returns a Promise
    // that resolves when all the 'promises' have resolved
    Promise.all(promises)
        .then(() => callback(null, `Processed ${event.Records.length} file(s)`))
        .catch(err => callback(err));
};

// Take the json input and transform it (by adding StatusTime)
// to appropriate format for DynamoDB
function buildRequests(buf) {
    // create a new array of type "item" by parsing the input file
    const requestItems = buf.toString().trim().split('\n').map((json) => {

        // you should check you are parsing a correct json record
        // and handle any errors
        const item = JSON.parse(json);
        item.StatusTime = (new Date(item.StatusTime)).getTime();

        // DynamoDB expects a JSON map with PutRequest
        //  - Perform a PutItem operation on the specified item.
        //   The item to be put is identified by an Item subelement:
        //   - Item - A map of attributes and their values.
        return {
            PutRequest: {
                Item: item,
            },
        };
    });
    // returns a Promise object that is resolved with the given value (requestItems)
    return Promise.resolve(requestItems);
}

// Take the correctly formatted items in the request,
// process them in batches of 25 at the time
function processRequests(requestItems) {
    const batches = [];

    while (requestItems.length > 0) {
        // process 25 records at the time
        console.log('remaining records to be processed: ' + requestItems.length );
        batches.push(writeRecords(requestItems.splice(0, 25)));
    }
    // makes sure all are processed
    return Promise.all(batches);
}

// Write to DynamoDb the requestItems (in JSON)
// handle Throttling erros
function writeRecords(requestItems) {
    const params = {
        RequestItems: {
            [TABLE_NAME]: requestItems,
        },
    };

    return new Promise((resolve, reject) => {

        // DynamoDB is busy so we need to slow down and retry
        function retry(retryRequestItems) {
            const delay = ((Math.random() * (3 - 1)) + 1) * 1000;

            console.log(`Retrying ${retryRequestItems.length} reqs in ${Math.round(delay)}ms`);
            setTimeout(() => {
                writeRecords(retryRequestItems).then(resolve).catch(reject);
            }, delay);
        }

        // The logic for DynamoDB.batchWrite() is funky, make sure
        // you read the documentation:
        // http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
        // because you may get errors for unprocessed data and for throttling
        DynamoDB.batchWrite(params, (err, data) => {
            if (data && data.UnprocessedItems[TABLE_NAME]) {
                retry(data.UnprocessedItems[TABLE_NAME]);
            } else if (err && THROTTLING_ERRORS.includes(err.name)) {
                retry(requestItems);
            } else if (err) {
                reject(err);
            } else {
                // all went well
                resolve();
            }
        });
    });
}

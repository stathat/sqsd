# sqsd

This is an extremely minimal server that acts like AWS SQS.

It works for us in our dev/testing environment, but the least amount of code was written to get it operational
and we took many shortcuts generating the responses (e.g. all `MessageId` and `RequestId` fields are the same).

Install:

    go get github.com/stathat/sqsd

Run:

    sqsd -queues buckets,jobs

It handles these SQS operations:

    GetQueueUrl
    ListQueues
    CreateQueue
    ReceiveMessage
    DeleteMessage
    SendMessage

Pretty much the only parameter supported is `WaitTimeSeconds` for `ReceiveMessage`.  

Tested with the goamz/sqs package as a client.

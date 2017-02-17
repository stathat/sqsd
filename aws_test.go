package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestAWSSDK(t *testing.T) {
	Setup()

	sess, err := session.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	svc := sqs.New(sess, aws.NewConfig().WithRegion("test").WithEndpoint(fmt.Sprintf("http://localhost:%d", *port)).WithDisableSSL(true))

	qname := "helloaws"
	_, err = svc.CreateQueue(&sqs.CreateQueueInput{QueueName: aws.String(qname)})
	if err != nil {
		t.Fatal(err)
	}

	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(qname)})
	if err != nil {
		t.Fatal(err)
	}
	qurl := resultURL.QueueUrl

	m := awsReceive(t, svc, qurl)
	if len(m.Messages) != 0 {
		t.Fatalf("messages: %d, expected 0", len(m.Messages))
	}

	body := "jjjjwwww"
	awsSend(t, svc, qurl, body)

	m = awsReceive(t, svc, qurl)
	if len(m.Messages) != 1 {
		t.Fatalf("messages: %d, expected 1", len(m.Messages))
	}
	if m.Messages[0].Body == nil {
		t.Fatal("nil body")
	}
	if *m.Messages[0].Body != body {
		t.Fatalf("body: %q, expected %q", *m.Messages[0].Body, body)
	}

	m = awsReceive(t, svc, qurl)
	if len(m.Messages) != 0 {
		t.Fatalf("messages: %d, expected 0", len(m.Messages))
	}

	body2 := "asdf"
	awsSend(t, svc, qurl, body2)
	m = awsReceive(t, svc, qurl)
	if len(m.Messages) != 1 {
		t.Fatalf("messages: %d, expected 1", len(m.Messages))
	}
	if m.Messages[0].Body == nil {
		t.Fatal("nil body")
	}
	if *m.Messages[0].Body != body2 {
		t.Fatalf("body: %q, expected %q", *m.Messages[0].Body, body2)
	}
	awsDeleteAll(t, svc, qurl, m)

	body3 := "zxcv"
	go func() {
		time.Sleep(20 * time.Millisecond)
		awsSend(t, svc, qurl, body3)
	}()
	m = awsReceiveWait(t, svc, qurl)
	if len(m.Messages) != 1 {
		t.Fatalf("messages: %d, expected 1", len(m.Messages))
	}
	if m.Messages[0].Body == nil {
		t.Fatal("nil body")
	}
	if *m.Messages[0].Body != body3 {
		t.Fatalf("body: %q, expected %q", *m.Messages[0].Body, body3)
	}

	params := &sqs.DeleteMessageBatchInput{
		Entries: []*sqs.DeleteMessageBatchRequestEntry{
			{
				Id:            m.Messages[0].MessageId,
				ReceiptHandle: m.Messages[0].ReceiptHandle,
			},
		},
		QueueUrl: qurl,
	}
	_, err = svc.DeleteMessageBatch(params)
	if err != nil {
		t.Fatal(err)
	}

}

func awsReceive(t *testing.T, svc *sqs.SQS, qurl *string) *sqs.ReceiveMessageOutput {
	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            qurl,
		MaxNumberOfMessages: aws.Int64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func awsReceiveWait(t *testing.T, svc *sqs.SQS, qurl *string) *sqs.ReceiveMessageOutput {
	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            qurl,
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func awsSend(t *testing.T, svc *sqs.SQS, qurl *string, body string) {
	_, err := svc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(body),
		QueueUrl:    qurl,
	})
	if err != nil {
		t.Fatal(err)
	}

}

func awsDeleteAll(t *testing.T, svc *sqs.SQS, qurl *string, r *sqs.ReceiveMessageOutput) {
	for _, mm := range r.Messages {
		_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      qurl,
			ReceiptHandle: mm.ReceiptHandle,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

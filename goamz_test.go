package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/sqs"
)

func TestGoamzSQS(t *testing.T) {
	Setup()

	r := aws.Region{SQSEndpoint: fmt.Sprintf("http://localhost:%d", *port)}
	s := sqs.New(aws.Auth{AccessKey: "x", SecretKey: "x"}, r)

	qname := "hello"
	_, err := s.CreateQueue(qname)
	if err != nil {
		t.Fatal(err)
	}

	q, err := s.GetQueue(qname)
	if err != nil {
		t.Fatal(err)
	}
	m := receive(t, q)
	if len(m.Messages) != 0 {
		t.Fatalf("messages: %d, expected 0", len(m.Messages))
	}

	body := "qoooo"
	send(t, q, body)
	m = receive(t, q)
	if len(m.Messages) != 1 {
		t.Fatalf("messages: %d, expected 1", len(m.Messages))
	}
	if m.Messages[0].Body != body {
		t.Fatalf("message body: %q, expected %q", m.Messages[0].Body, body)
	}

	m = receive(t, q)
	if len(m.Messages) != 0 {
		t.Fatalf("messages: %d, expected 0", len(m.Messages))
	}

	body2 := "asdf"
	send(t, q, body2)
	m = receive(t, q)
	if len(m.Messages) != 1 {
		t.Fatalf("messages: %d, expected 1", len(m.Messages))
	}
	if m.Messages[0].Body != body2 {
		t.Fatalf("message body: %q, expected %q", m.Messages[0].Body, body2)
	}
	deleteAll(t, q, m)

	body3 := "zxcv"
	go func() {
		time.Sleep(20 * time.Millisecond)
		send(t, q, body3)
	}()
	m = receiveWait(t, q)
	if len(m.Messages) != 1 {
		t.Fatalf("messages: %d, expected 1", len(m.Messages))
	}
	if m.Messages[0].Body != body3 {
		t.Fatalf("message body: %q, expected %q", m.Messages[0].Body, body3)
	}
}

func send(t *testing.T, q *sqs.Queue, body string) {
	_, err := q.SendMessage(body)
	if err != nil {
		t.Fatal(err)
	}
}

func receive(t *testing.T, q *sqs.Queue) *sqs.ReceiveMessageResponse {
	m, err := q.ReceiveMessage(1)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func receiveWait(t *testing.T, q *sqs.Queue) *sqs.ReceiveMessageResponse {
	params := map[string]string{
		"MaxNumberOfMessages": "1",
		"WaitTimeSeconds":     "2",
	}
	m, err := q.ReceiveMessageWithParameters(params)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func deleteAll(t *testing.T, q *sqs.Queue, r *sqs.ReceiveMessageResponse) {
	for _, mm := range r.Messages {
		_, err := q.DeleteMessage(&mm)
		if err != nil {
			t.Fatal(err)
		}
	}
}

package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"html"
	"log"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

var port = flag.Int("port", 9324, "port")
var cqueues = flag.String("queues", "", "queues to create on startup, comma-separated")

type msg struct {
	body    string
	handles []string
}

func (m *msg) XML() string {
	return fmt.Sprintf("<Message><MessageId>5fea7756-0ea4-451a-a703-a558b933e274</MessageId><ReceiptHandle>%s</ReceiptHandle><MD5OfBody></MD5OfBody><Body>%s</Body></Message>", m.handles[0], m.body)
}

func (m *msg) AddHandle() string {
	b := make([]byte, 10)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	h := hex.EncodeToString(b)
	m.handles = append(m.handles, h)
	return h
}

type queue struct {
	name      string
	msgs      chan *msg
	delivered chan *msg
}

func newQueue(name string) *queue {
	return &queue{
		name:      name,
		msgs:      make(chan *msg, 10000),
		delivered: make(chan *msg, 10000),
	}
}

func (q *queue) URL() string {
	return fmt.Sprintf("http://localhost:%d/123/%s", *port, q.name)
}

func (q *queue) Front() *msg {
	select {
	case m := <-q.msgs:
		m.AddHandle()
		q.delivered <- m
		return m
	default:
		return nil
	}
}

func (q *queue) FrontWait(secs int) *msg {
	var m *msg
	select {
	case m = <-q.msgs:
		m.AddHandle()
		q.delivered <- m
		return m
	case <-time.After(time.Duration(secs) * time.Second):
		return nil
	}

	return nil
}

func (q *queue) Enqueue() {
}

var queues map[string]*queue
var queuesMu sync.Mutex

func getQueueURL(w http.ResponseWriter, r *http.Request) {
	log.Printf("GetQueueUrl")
	name := r.FormValue("QueueName")
	var url string
	queuesMu.Lock()
	q, ok := queues[name]
	if ok {
		url = q.URL()
	}
	queuesMu.Unlock()

	fmt.Fprintf(w, "<GetQueueUrlResponse><GetQueueUrlResult><QueueUrl>%s</QueueUrl></GetQueueUrlResult><ResponseMetadata><RequestId>470a6f13-2ed9-4181-ad8a-2fdea142988e</RequestId></ResponseMetadata></GetQueueUrlResponse>", url)
}

func listQueues(w http.ResponseWriter, r *http.Request) {
	log.Printf("ListQueues")
	fmt.Fprintf(w, " <ListQueuesResponse> <ListQueuesResult> ")
	queuesMu.Lock()
	defer queuesMu.Unlock()
	for _, q := range queues {
		fmt.Fprintf(w, " <QueueUrl> %s </QueueUrl> ", q.URL())
	}
	fmt.Fprintf(w, " </ListQueuesResult> <ResponseMetadata> <RequestId> 725275ae-0b9b-4762-b238-436d7c65a1ac </RequestId> </ResponseMetadata> </ListQueuesResponse>")
}

func createQueue(w http.ResponseWriter, r *http.Request) {
	log.Printf("CreateQueue")
	name := r.FormValue("QueueName")
	q := newQueue(name)
	queuesMu.Lock()
	queues[name] = q
	queuesMu.Unlock()

	fmt.Fprintf(w, "<CreateQueueResponse> <CreateQueueResult> <QueueUrl>%s</QueueUrl> </CreateQueueResult> <ResponseMetadata> <RequestId> 7a62c49f-347e-4fc4-9331-6e8e7a96aa73 </RequestId> </ResponseMetadata> </CreateQueueResponse>", q.URL())
}

func receiveMessage(w http.ResponseWriter, r *http.Request) {
	qname := path.Base(r.URL.Path)
	queuesMu.Lock()
	q, ok := queues[qname]
	queuesMu.Unlock()
	if !ok {
		log.Printf("no queue %q", qname)
		return
	}

	fmt.Fprintf(w, "<ReceiveMessageResponse><ReceiveMessageResult>")

	var secs int
	if r.FormValue("WaitTimeSeconds") != "" {
		var err error
		secs, err = strconv.Atoi(r.FormValue("WaitTimeSeconds"))
		if err != nil {
			secs = 0
		}
	}
	var m *msg
	if secs == 0 {
		m = q.Front()
	} else {
		m = q.FrontWait(secs)
	}
	if m != nil {
		log.Printf("ReceiveMessage -> %q returning message", qname)
		fmt.Fprintf(w, m.XML())
	}
	fmt.Fprintf(w, "</ReceiveMessageResult><ResponseMetadata><RequestId>b6633655-283d-45b4-aee4-4e84e0ae6afa</RequestId></ResponseMetadata></ReceiveMessageResponse>")
}

func deleteMessage(w http.ResponseWriter, r *http.Request) {
	qname := path.Base(r.URL.Path)
	log.Printf("DeleteMessage -> %q", qname)
	queuesMu.Lock()
	q, ok := queues[qname]
	queuesMu.Unlock()
	if !ok {
		log.Printf("no queue %q", qname)
		return
	}
	rh := r.FormValue("ReceiptHandle")
	found := false
	for m := range q.delivered {
		for _, h := range m.handles {
			if h == rh {
				found = true
				break
			}
		}
		if found {
			break
		}
		q.delivered <- m
	}
	if found {
		fmt.Fprintf(w, "<DeleteMessageResponse><ResponseMetadata><RequestId>b5293cb5-d306-4a17-9048-b263635abe42</RequestId></ResponseMetadata></DeleteMessageResponse>")
		return
	}
	http.Error(w, "400 invalid receipt handle", http.StatusBadRequest)
}

func sendMessage(w http.ResponseWriter, r *http.Request) {
	qname := path.Base(r.URL.Path)
	log.Printf("SendMessage -> %q", qname)
	queuesMu.Lock()
	defer queuesMu.Unlock()
	q, ok := queues[qname]
	if !ok {
		log.Printf("no queue %q", qname)
		return
	}
	q.msgs <- &msg{body: r.FormValue("MessageBody")}
	fmt.Fprintf(w, `<SendMessageResponse>
    <SendMessageResult>
        <MD5OfMessageBody>
            fafb00f5732ab283681e124bf8747ed1
        </MD5OfMessageBody>
        <MD5OfMessageAttributes>
	    3ae8f24a165a8cedc005670c81a27295
        </MD5OfMessageAttributes>
        <MessageId>
            5fea7756-0ea4-451a-a703-a558b933e274
        </MessageId>
    </SendMessageResult>
    <ResponseMetadata>
        <RequestId>
            27daac76-34dd-47df-bd01-1f6e873584a0
        </RequestId>
    </ResponseMetadata>
</SendMessageResponse>`)
}

func handler(w http.ResponseWriter, r *http.Request) {
	a := r.FormValue("Action")
	switch a {
	case "GetQueueUrl":
		getQueueURL(w, r)
	case "ListQueues":
		listQueues(w, r)
	case "CreateQueue":
		createQueue(w, r)
	case "ReceiveMessage":
		receiveMessage(w, r)
	case "DeleteMessage":
		deleteMessage(w, r)
	case "SendMessage":
		sendMessage(w, r)
	default:
		log.Printf("unknown action: %q", a)
		log.Printf("request: %+v", r)
	}

	fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
}

func main() {
	log.Printf("sqs memory server starting")
	flag.Parse()

	queues = make(map[string]*queue)

	cqnames := strings.Split(*cqueues, ",")
	queuesMu.Lock()
	for _, name := range cqnames {
		log.Printf("creating queue %q", name)
		q := newQueue(name)
		queues[name] = q
	}
	queuesMu.Unlock()

	http.HandleFunc("/", handler)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}

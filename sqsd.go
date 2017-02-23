package main

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

var host = flag.String("host", "localhost", "hostname")
var port = flag.Int("port", 9324, "port")
var cqueues = flag.String("queues", "", "queues to create on startup, comma-separated")
var delay = flag.Duration("delay", 0, "delay for each message")

type msg struct {
	body    string
	handles []string
}

func (m *msg) XML() string {
	hash := md5.Sum([]byte(m.body))
	return fmt.Sprintf("<Message><MessageId>5fea7756-0ea4-451a-a703-a558b933e274</MessageId><ReceiptHandle>%s</ReceiptHandle><MD5OfBody>%x</MD5OfBody><Body>%s</Body></Message>", m.handles[0], hash, m.body)
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
	return fmt.Sprintf("http://%s:%d/123/%s", *host, *port, q.name)
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
	name := r.FormValue("QueueName")
	var url string
	queuesMu.Lock()
	q, ok := queues[name]
	if ok {
		url = q.URL()
	}
	queuesMu.Unlock()
	log.Printf("GetQueueUrl [%s] -> %s", name, url)

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
	name := r.FormValue("QueueName")
	log.Printf("CreateQueue [%s]", name)
	q := newQueue(name)
	queuesMu.Lock()
	queues[name] = q
	queuesMu.Unlock()

	fmt.Fprintf(w, "<CreateQueueResponse> <CreateQueueResult> <QueueUrl>%s</QueueUrl> </CreateQueueResult> <ResponseMetadata> <RequestId> 7a62c49f-347e-4fc4-9331-6e8e7a96aa73 </RequestId> </ResponseMetadata> </CreateQueueResponse>", q.URL())
}

func findQueue(r *http.Request) (*queue, string, bool) {
	var name string
	if r.Method == "GET" {
		name = path.Base(r.URL.Path)
	} else {
		qurl := r.FormValue("QueueUrl")
		name = path.Base(qurl)
	}
	queuesMu.Lock()
	q, ok := queues[name]
	queuesMu.Unlock()

	return q, name, ok
}

func receiveMessage(w http.ResponseWriter, r *http.Request) {
	q, qname, ok := findQueue(r)
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
		log.Printf("ReceiveMessage [%s] => (%s)", qname, m.body)
		fmt.Fprintf(w, m.XML())
	}
	fmt.Fprintf(w, "</ReceiveMessageResult><ResponseMetadata><RequestId>b6633655-283d-45b4-aee4-4e84e0ae6afa</RequestId></ResponseMetadata></ReceiveMessageResponse>")
}

func deleteMessage(w http.ResponseWriter, r *http.Request) {
	q, qname, ok := findQueue(r)
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

func deleteMessageBatch(w http.ResponseWriter, r *http.Request) {
	q, qname, ok := findQueue(r)
	if !ok {
		log.Printf("no queue %q", qname)
		return
		_ = q
	}

	i := 1
	for {
		rh := r.FormValue(fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.ReceiptHandle", i))
		if rh == "" {
			break
		}

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
		if !found {
			http.Error(w, "400 invalid receipt handle", http.StatusBadRequest)
			return
		}

		i++
	}

	fmt.Fprintf(w, "<DeleteMessageBatchResponse><ResponseMetadata><RequestId>b5293cb5-d306-4a17-9048-b263635abe42</RequestId></ResponseMetadata></DeleteMessageBatchResponse>")
}

func sendMessage(w http.ResponseWriter, r *http.Request) {
	q, qname, ok := findQueue(r)
	if !ok {
		log.Printf("no queue %q", qname)
		return
	}
	body := r.FormValue("MessageBody")
	log.Printf("SendMessage => [%s] (%s)", qname, body)
	if *delay > 0 {
		go func() {
			log.Printf("SendMessage [%s] <= (%s) delay %s", qname, body, *delay)
			time.Sleep(*delay)
			q.msgs <- &msg{body: body}
			log.Printf("SendMessage [%s] <= (%s) sent", qname, body)
		}()
	} else {
		log.Printf("SendMessage [%s] <= (%s)", qname, body)
		q.msgs <- &msg{body: body}
		log.Printf("SendMessage [%s] <= (%s) sent", qname, body)
	}
	hash := md5.Sum([]byte(body))
	fmt.Fprintf(w, `<SendMessageResponse><SendMessageResult><MD5OfMessageBody>%x</MD5OfMessageBody><MD5OfMessageAttributes>3ae8f24a165a8cedc005670c81a27295</MD5OfMessageAttributes><MessageId>5fea7756-0ea4-451a-a703-a558b933e274</MessageId></SendMessageResult><ResponseMetadata><RequestId>27daac76-34dd-47df-bd01-1f6e873584a0</RequestId></ResponseMetadata></SendMessageResponse>`, hash)
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
	case "DeleteMessageBatch":
		deleteMessageBatch(w, r)
	case "SendMessage":
		sendMessage(w, r)
	default:
		log.Printf("unknown action: %q", a)
		log.Printf("request: %+v", r)
	}
}

func reset() {
	queuesMu.Lock()
	queues = make(map[string]*queue)
	queuesMu.Unlock()
}

func run(qlist string) {
	reset()

	cqnames := strings.Split(qlist, ",")
	queuesMu.Lock()
	for _, name := range cqnames {
		if len(name) == 0 {
			continue
		}
		log.Printf("creating queue %q", name)
		q := newQueue(name)
		queues[name] = q
	}
	queuesMu.Unlock()

	http.HandleFunc("/", handler)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}

func main() {
	log.Printf("sqs memory server starting")
	flag.Parse()
	run(*cqueues)
}

package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"
)

type Worker struct {
	id        int
	requests  chan *StoredRequest
	serverURL *url.URL
	client    *http.Client
	store     RequestStore
}

func newWorker(id int,
	requests chan *StoredRequest,
	serverURL *url.URL,
	client *http.Client,
	store RequestStore) *Worker {
	return &Worker{
		id:        id,
		requests:  requests,
		serverURL: serverURL,
		client:    client,
		store:     store,
	}
}

func (w *Worker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-w.requests:
			w.send(r)
		}
	}
}

func (w *Worker) send(s *StoredRequest) {
	url, err := url.Parse(w.serverURL.String())
	if err != nil {
		log.Printf("Could not parse URL: %v", err)
		w.reschedule(s)
	}
	url.Path = s.Path
	req, err := http.NewRequest(s.Method, url.String(), nil)
	if err != nil {
		log.Fatalf("Could not create request: %v", s)
	}
	req.Header = s.Headers
	req.Body = ioutil.NopCloser(bytes.NewReader(s.Body))
	res, err := w.client.Do(req)
	if err != nil {
		log.Printf("Could not get response: %v", err)
		w.reschedule(s)
		return
	}
	if res.StatusCode != 200 {
		log.Printf("Bad response. Request: %v. Status:: %d", s, res.StatusCode)
		w.reschedule(s)
		return
	}
	err = w.store.Delete(s)
	if err != nil {
		log.Fatalf("Could not delete stored request: %v", err)
	}
}

func (w *Worker) reschedule(s *StoredRequest) {
	if s.TTL == 1 {
		log.Printf("Request expired: %v\n", s)
		err := w.store.Delete(s)
		if err != nil {
			log.Fatalf("Could not delete request: %v", err)
		}
		return
	}

	rescheduled := &StoredRequest{
		UID: s.UID,
		// TODO: Use a parameterized exponential backoff
		DeliveryTime: time.Now().Add(10 * time.Second).UnixNano(),
		Path:         s.Path,
		Method:       s.Method,
		Body:         s.Body,
		Headers:      s.Headers,
		Scheduled:    false,
	}

	if s.TTL > 1 {
		rescheduled.TTL = s.TTL - 1
	}

	err := w.store.Reschedule(s, rescheduled)
	if err != nil {
		log.Fatalf("Could not store rescheduled request: %v", err)
	}
}

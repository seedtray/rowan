package main

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"strconv"
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
	res, err := w.client.Do(req)
	if err != nil {
		log.Printf("Could not send request: %v", err)
		// TODO: How should we report these errors?
		OutboundHTTPRequests.WithLabelValues("400").Inc()
		w.reschedule(s)
		return
	}

	latency := time.Since(time.Unix(0, s.DeliveryTime))
	statusLabel := strconv.Itoa(res.StatusCode)
	OutboundHTTPRequests.WithLabelValues(statusLabel).Inc()
	OutboundHTTPLatencies.WithLabelValues(statusLabel).Observe(latency.Seconds())
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
	rescheduled := &StoredRequest{
		UID: s.UID,
		// TODO: Use a parameterized exponential backoff
		DeliveryTime: s.DeliveryTime + int64(time.Second*10),
		Path:         s.Path,
		Method:       s.Method,
		Scheduled:    false,
	}

	if s.TTL == 1 {
		log.Printf("Request expired: %v\n", s)
		err := w.store.Delete(s)
		if err != nil {
			log.Fatalf("Could not delete request: %v", err)
		}
		return
	}

	if s.TTL > 0 {
		rescheduled.TTL = s.TTL - 1
	}

	err := w.store.Reschedule(s, rescheduled)
	if err != nil {
		log.Fatalf("Could not store rescheduled request: %v", err)
	}
}

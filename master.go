package main

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type Master struct {
	workers  []*Worker
	store    RequestStore
	requests chan *StoredRequest
}

func newMaster(maxConcurrentRequests uint,
	serverURL *url.URL,
	store RequestStore,
	httpClient *http.Client) *Master {
	m := &Master{
		requests: make(chan *StoredRequest),
		store:    store,
	}
	m.workers = make([]*Worker, maxConcurrentRequests)
	for i := 0; i < len(m.workers); i++ {
		m.workers[i] = newWorker(i, m.requests, serverURL, httpClient, store)
	}
	return m
}

func (m *Master) run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, w := range m.workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			w.run(ctx)
		}(w)
	}
	// TODO: Recover 'scheduled' requests from an ungraceful shutdown
	for {
		select {
		case <-ctx.Done():
			// We are shutting down so we need to drain all scheduled messages
			// by waiting for all workers to finish.
			wg.Wait()
			return
		default:
			m.scheduleNext()
		}
	}
}

func (m *Master) scheduleNext() {
	next, err := m.store.Next(len(m.workers) * 2)
	if err != nil {
		log.Fatalf("Could not read from store: %v", err)
	}
	enqueued := false
	now := time.Now().UnixNano()
	for _, r := range next {
		if r.DeliveryTime > now {
			break
		}
		if r.Scheduled {
			continue
		}
		r.Scheduled = true
		m.store.Put(r)
		m.requests <- r
		enqueued = true
	}
	if !enqueued {
		// TODO: Don't do this awful busy wait. A better approach would
		// be to select on a third channel that notifies new incoming requests.
		time.Sleep(100 * time.Millisecond)
	}
}

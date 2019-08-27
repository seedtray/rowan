package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	// All service flags.
	baseURL := flag.String("base_url", "http://example.com", "")
	clientTimeout := flag.Duration("client_timeout", time.Duration(10 * time.Second), "")
	maxConcurrentRequests := flag.Uint("max_concurrent_requests", 10, "")
	port := flag.Int("port", 8080, "")

	flag.Parse()

	server, err := newServer(*maxConcurrentRequests, *baseURL, *clientTimeout)
	if err != nil {
		log.Fatalf("Could not initialize server: %v", err)
	}

	s := &http.Server{
		Addr:           fmt.Sprintf(":%d", *port),
		Handler:        server,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt)
		signal.Notify(stop, syscall.SIGTERM)
		// Block until SIGINT/SIGTERM.
		<-stop
		log.Println("Shutting down service")
		if err := s.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
		cancel()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		server.master.run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	wg.Wait()
	log.Println("Service shutdown completed")
}

type Server struct {
	store                 RequestStore
	master                *Master
	maxConcurrentRequests uint
	serverURL             *url.URL
}

func newServer(maxConcurrentRequests uint, baseURL string, clientTimeout time.Duration) (*Server, error) {
	s, err := newBoltRequestStore("requests.db")
	if err != nil {
		return nil, err
	}
	serverURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{
		Timeout: clientTimeout,
	}
	return &Server{
		store:                 s,
		maxConcurrentRequests: maxConcurrentRequests,
		serverURL:             serverURL,
		master:                newMaster(maxConcurrentRequests, serverURL, s, httpClient),
	}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	uid := make([]byte, 8)
	if _, err := rand.Read(uid); err != nil {
		log.Printf("Could not generate random UID: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Could not read request body: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	sr := &StoredRequest{
		UID: uid,
		// TODO: Allow custom delivery time from header.
		DeliveryTime: time.Now().UnixNano(),
		Path:         r.URL.Path,
		Method:       r.Method,
		Headers:      r.Header,
		Body:         body,
		// TODO: Read from header.
		TTL: 3,
		//TODO: Add support for body and headers.
	}
	if err := s.store.Put(sr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

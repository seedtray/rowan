package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	// All service flags.
	baseURL := flag.String("base_url", "http://example.com", "")
	maxConcurrentRequests := flag.Uint("max_concurrent_requests", 10, "")
	inboundPort := flag.Int("inbound_port", 8080, "")
	metricsPort := flag.Int("metrics_port", 8081, "")

	flag.Parse()

	go serveMetrics(*metricsPort)

	server, err := newServer(*maxConcurrentRequests, *baseURL)
	if err != nil {
		log.Fatalf("Could not initialize server: %v", err)
	}

	s := &http.Server{
		Addr:           fmt.Sprintf(":%d", *inboundPort),
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
			log.Print(err)
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

func newServer(maxConcurrentRequests uint, baseURL string) (*Server, error) {
	s, err := newBoltRequestStore("requests.db")
	if err != nil {
		return nil, err
	}
	serverURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	httpClient := http.DefaultClient
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
		log.Printf("Could not generate UID: %v", err)
		s.writeError(w)
		return
	}
	sr := &StoredRequest{
		UID: uid,
		// TODO: Allow custom delivery time from header.
		DeliveryTime: time.Now().UnixNano(),
		Path:         r.URL.Path,
		Method:       r.Method,
		// TODO: Read from header.
		TTL: 3,
		//TODO: Add support for body and headers.
	}
	if err := s.store.Put(sr); err != nil {
		log.Printf("Could not persist request: %v", err)
		s.writeError(w)
		return
	}

	InboundHTTPRequests.WithLabelValues("200").Inc()
}

func (s *Server) writeError(w http.ResponseWriter) {
	status := http.StatusInternalServerError
	w.WriteHeader(status)
	InboundHTTPRequests.WithLabelValues(strconv.Itoa(status)).Inc()
}

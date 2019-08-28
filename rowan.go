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
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	// All service flags.
	baseURL := flag.String("base_url", "http://example.com", "")
	clientTimeout := flag.Duration("client_timeout", time.Duration(10*time.Second), "")
	maxConcurrentRequests := flag.Uint("max_concurrent_requests", 10, "")
	inboundPort := flag.Int("inbound_port", 8080, "")
	metricsPort := flag.Int("metrics_port", 8081, "")
	storagePath := flag.String("storage_path", "data.db", "")

	flag.Parse()

	go serveMetrics(*metricsPort)
	server, err := newServer(
		*maxConcurrentRequests,
		*baseURL,
		*clientTimeout,
		*storagePath)

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

func newServer(maxConcurrentRequests uint,
	baseURL string,
	clientTimeout time.Duration,
	storagePath string) (*Server, error) {
	s, err := newBoltRequestStore(storagePath)
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
		UID:          uid,
		DeliveryTime: time.Now().UnixNano(),
		Path:         r.URL.Path,
		Method:       r.Method,
		Headers:      r.Header,
		Body:         body,
	}

	ttlHeader := r.Header.Get("X-Rowan-Ttl")
	if ttlHeader != "" {
		ttl, err := strconv.ParseInt(ttlHeader, 10, 64)
		if err != nil {
			writeResponse(w,
				http.StatusBadRequest,
				"Could not parse X-Rowan-Ttl header")
			return
		}
		sr.TTL = ttl
	}

	deliveryTimeHeader := r.Header.Get("X-Rowan-Deliverytime")
	if deliveryTimeHeader != "" {
		deliveryTime, err := time.Parse(time.RFC3339, deliveryTimeHeader)
		if err != nil {
			writeResponse(w,
				http.StatusBadRequest,
				"Could not parse X-Rowan-Deliverytime header")
			return
		}
		sr.DeliveryTime = deliveryTime.UnixNano()
	}

	if err := s.store.Put(sr); err != nil {
		writeResponse(w,
			http.StatusInternalServerError,
			"Internal Server Error")
	}
}

func writeResponse(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	if _, err := w.Write([]byte(msg)); err != nil {
		log.Printf("Could not write response body: %v", err)
		InboundHTTPRequests.WithLabelValues("500").Inc()
		return
	}

	InboundHTTPRequests.WithLabelValues(strconv.Itoa(status)).Inc()
}

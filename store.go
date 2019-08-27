package main

import (
	"encoding/binary"
	"encoding/json"

	"github.com/boltdb/bolt"
)

const requestsBucketName = "Requests"

type StoredRequest struct {
	// A globally unique synthetic id.
	UID []byte
	// The number of nanoseconds elapsed since Unix epoch as returned by time.UnixNano()
	DeliveryTime int64
	// The HTTP path to use when delibery this request.
	Path string
	// The HTTP method to use.
	Method  string
	// The HTTP headers to propagate.
	Headers map[string][]string
	// The request body to include.
	Body    []byte
	// Whether this request is being processed by a worker or not.
	Scheduled bool
	// How many times should I try to deliver this request before dropping it?
	TTL int
}

type RequestStore interface {
	Put(r *StoredRequest) error
	Next(n int) ([]*StoredRequest, error)
	Reschedule(current *StoredRequest, next *StoredRequest) error
	Delete(r *StoredRequest) error
}

type BoltRequestStore struct {
	db *bolt.DB
}

var _ RequestStore = (*BoltRequestStore)(nil)

func newBoltRequestStore(filename string) (*BoltRequestStore, error) {
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(requestsBucketName))
		return err
	})
	if err != nil {
		return nil, err
	}
	return &BoltRequestStore{
		db: db,
	}, nil
}

func (s *BoltRequestStore) Put(r *StoredRequest) error {
	v, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		key := getKey(r)
		return tx.Bucket([]byte(requestsBucketName)).Put(key, v)
	})
}

func (s *BoltRequestStore) Next(n int) ([]*StoredRequest, error) {
	var r []*StoredRequest
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(requestsBucketName))
		c := b.Cursor()
		i := 0
		for k, v := c.First(); k != nil && i < n; k, v = c.Next() {
			var sr StoredRequest
			err := json.Unmarshal(v, &sr)
			if err != nil {
				return err
			}
			if sr.Scheduled {
				continue
			}
			r = append(r, &sr)
			i = i + 1
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s *BoltRequestStore) Reschedule(current *StoredRequest, next *StoredRequest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		v, err := json.Marshal(next)
		if err != nil {
			return err
		}
		b := tx.Bucket([]byte(requestsBucketName))
		if err := b.Delete(getKey(current)); err != nil {
			return err
		}
		return b.Put(getKey(next), v)
	})
}

func (s *BoltRequestStore) Delete(r *StoredRequest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		key := getKey(r)
		b := tx.Bucket([]byte(requestsBucketName))
		return b.Delete(key)
	})
}

func getKey(r *StoredRequest) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key, uint64(r.DeliveryTime))
	copy(key[8:], r.UID)
	return key
}

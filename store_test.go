package main

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

type testStore struct {
	tmpFilePath string
	db          *BoltRequestStore
}

func newTestStore() (*testStore, error) {
	var t testStore
	tmpFile, err := ioutil.TempFile("", "tmp_store")
	if err != nil {
		return &t, err
	}
	t.tmpFilePath = tmpFile.Name()
	db, err := newBoltRequestStore(tmpFile.Name())
	if err != nil {
		return &t, err
	}
	t.db = db
	return &t, nil
}

func (t *testStore) tearDown() {
	if t.tmpFilePath != "" {
		os.Remove(t.tmpFilePath)
	}
}

func TestPut(t *testing.T) {
	s, err := newTestStore()
	defer s.tearDown()
	if err != nil {
		t.Fatalf("Could not create test store: %v", err)
	}

	err = s.db.Put(&StoredRequest{
		UID:          []byte{1},
		DeliveryTime: 1,
		Path:         "/foo",
		Headers:      map[string][]string{"X-Foo": []string{"Bar"}},
		Body:         []byte("Body content"),
		Method:       "GET",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	all, err := s.db.Next(1)
	if err != nil {
		t.Fatalf("Failed to Scan requests: %v", err)
	}

	if len(all) != 1 {
		t.Fatalf("Invalid number of requests. Expexted 1 but got %d", len(all))
	}

	actual := all[0]
	if actual.DeliveryTime != 1 {
		t.Fatalf("Wrong delivery time. Expected 1 but got %d", actual.DeliveryTime)
	}
}

func TestDelete(t *testing.T) {
	s, err := newTestStore()
	defer s.tearDown()
	if err != nil {
		t.Fatal(err)
	}

	req1 := &StoredRequest{
		UID:          []byte{1},
		DeliveryTime: 100,
		Path:         "/foo",
		Method:       "GET",
	}

	req2 := &StoredRequest{
		UID:          []byte{2},
		DeliveryTime: 223,
		Path:         "/bar",
		Method:       "POST",
	}

	for _, r := range []*StoredRequest{req2, req1} {
		if err = s.db.Put(r); err != nil {
			t.Fatal(err)
		}
	}

	if err = s.db.Delete(req2); err != nil {
		t.Fatal(err)
	}

	want := []*StoredRequest{req1}
	got, err := s.db.Next(2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got requests %v, want %v", got, want)
	}

}

func TestNext(t *testing.T) {
	s, err := newTestStore()
	defer s.tearDown()
	if err != nil {
		t.Fatal(err)
	}

	req1 := &StoredRequest{
		UID:          []byte{1},
		DeliveryTime: 100,
		Path:         "foo",
		Method:       "GET",
	}

	req2 := &StoredRequest{
		UID:          []byte{2},
		DeliveryTime: 223,
		Path:         "bar",
		Method:       "POST",
		Scheduled:    true,
	}

	req3 := &StoredRequest{
		UID:          []byte{3},
		DeliveryTime: 321,
		Path:         "baz",
		Method:       "GET",
	}

	for _, r := range []*StoredRequest{req2, req3, req1} {
		if err = s.db.Put(r); err != nil {
			t.Fatal(err)
		}
	}

	want := []*StoredRequest{req1}
	got, err := s.db.Next(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got requests %v, want %v", got, want)
	}

	want = []*StoredRequest{req1, req3}
	got, err = s.db.Next(2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got requests %v, want %v", got, want)
	}

	want = []*StoredRequest{req1, req3}
	got, err = s.db.Next(3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got requests %v, want %v", got, want)
	}
}

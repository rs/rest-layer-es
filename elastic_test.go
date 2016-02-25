package es

import (
	"encoding/json"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/stretchr/testify/assert"

	"gopkg.in/olivere/elastic.v3"
)

var now = time.Now()
var nowStr = now.Format(time.RFC3339Nano)

// cleanup deletes an index immediately and on defer when call as:
//
//   defer cleanup(c, "index")()
func cleanup(c *elastic.Client, index string) func() {
	c.DeleteIndex(index).Do()
	return func() {
		c.DeleteIndex(index).Do()
	}
}

func TestInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	c, err := elastic.NewClient()
	if !assert.NoError(t, err) {
		return
	}
	defer cleanup(c, "testinsert")()
	h := NewHandler(c, "testinsert", "test")
	items := []*resource.Item{
		{
			ID:      "1234",
			ETag:    "etag",
			Updated: now,
			Payload: map[string]interface{}{
				"foo": "bar",
			},
		},
	}
	err = h.Insert(context.Background(), items)
	assert.NoError(t, err)
	res, err := c.Get().Index("testinsert").Type("test").Id("1234").Do()
	if !assert.NoError(t, err) {
		return
	}
	d := map[string]interface{}{}
	err = json.Unmarshal(*res.Source, &d)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, map[string]interface{}{"foo": "bar", "_etag": "etag", "_updated": nowStr}, d)

	// Inserting same item twice should return a conflict error
	err = h.Insert(context.Background(), items)
	assert.Equal(t, resource.ErrConflict, err)
}

func TestUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	c, err := elastic.NewClient()
	if !assert.NoError(t, err) {
		return
	}
	defer cleanup(c, "testupdate")()
	h := NewHandler(c, "testupdate", "test")
	oldItem := &resource.Item{
		ID:      "1234",
		ETag:    "etag1",
		Updated: now,
		Payload: map[string]interface{}{
			"foo": "bar",
		},
	}
	newItem := &resource.Item{
		ID:      "1234",
		ETag:    "etag2",
		Updated: now,
		Payload: map[string]interface{}{
			"foo": "baz",
		},
	}

	// Can't update a non existing item
	err = h.Update(context.Background(), newItem, oldItem)
	assert.Equal(t, resource.ErrNotFound, err)

	err = h.Insert(context.Background(), []*resource.Item{oldItem})
	assert.NoError(t, err)
	err = h.Update(context.Background(), newItem, oldItem)
	assert.NoError(t, err)

	// Update refused if original item's etag doesn't match stored one
	err = h.Update(context.Background(), newItem, oldItem)
	assert.Equal(t, resource.ErrConflict, err)
}

func TestDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	c, err := elastic.NewClient()
	if !assert.NoError(t, err) {
		return
	}
	defer cleanup(c, "testdelete")()
	h := NewHandler(c, "testdelete", "test")
	item := &resource.Item{
		ID:      "1234",
		ETag:    "etag1",
		Updated: now,
		Payload: map[string]interface{}{
			"foo": "bar",
		},
	}

	// Can't delete a non existing item
	err = h.Delete(context.Background(), item)
	assert.Equal(t, resource.ErrNotFound, err)

	err = h.Insert(context.Background(), []*resource.Item{item})
	assert.NoError(t, err)
	err = h.Delete(context.Background(), item)
	assert.NoError(t, err)

	// Update refused if original item's etag doesn't match stored one
	err = h.Insert(context.Background(), []*resource.Item{item})
	assert.NoError(t, err)
	item.ETag = "etag2"
	err = h.Delete(context.Background(), item)
	assert.Equal(t, resource.ErrConflict, err)
}

func TestClear(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	t.Skip("clear doesn't work yet")
	c, err := elastic.NewClient()
	if !assert.NoError(t, err) {
		return
	}
	// defer cleanup(c, "testclear")()
	h := NewHandler(c, "testclear", "test")
	items := []*resource.Item{
		{
			ID: "1",
			Payload: map[string]interface{}{
				"name": "a",
			},
		},
		{
			ID: "2",
			Payload: map[string]interface{}{
				"name": "b",
			},
		},
		{
			ID: "3",
			Payload: map[string]interface{}{
				"name": "c",
			},
		},
		{
			ID: "4",
			Payload: map[string]interface{}{
				"name": "d",
			},
		},
	}

	err = h.Insert(context.Background(), items)
	assert.NoError(t, err)

	lookup := resource.NewLookup()
	lookup.AddQuery(schema.Query{schema.In{Field: "name", Values: []schema.Value{"c", "d"}}})
	deleted, err := h.Clear(context.Background(), lookup)
	assert.NoError(t, err)
	assert.Equal(t, 2, deleted)

	lookup = resource.NewLookup()
	lookup.AddQuery(schema.Query{schema.Equal{Field: "id", Value: "2"}})
	deleted, err = h.Clear(context.Background(), lookup)
	assert.NoError(t, err)
	assert.Equal(t, 1, deleted)
}

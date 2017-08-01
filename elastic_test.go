package es

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v5"
)

var now = time.Now()
var nowStr = now.Format(time.RFC3339Nano)

// cleanup deletes an index immediately and on defer when call as:
//
//   defer cleanup(c, "index")()
func cleanup(c *elastic.Client, index string) func() {
	ctx := context.TODO()
	c.DeleteIndex(index).Do(ctx)
	return func() {
		c.DeleteIndex(index).Do(ctx)
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
				"id":  "1234",
				"foo": "bar",
			},
		},
	}
	ctx := context.TODO()
	err = h.Insert(ctx, items)
	assert.NoError(t, err)
	res, err := c.Get().Index("testinsert").Type("test").Id("1234").Do(ctx)
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
	err = h.Insert(ctx, items)
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
			"id":  "1234",
			"foo": "bar",
		},
	}
	newItem := &resource.Item{
		ID:      "1234",
		ETag:    "etag2",
		Updated: now,
		Payload: map[string]interface{}{
			"id":  "1234",
			"foo": "baz",
		},
	}

	// Can't update a non existing item
	ctx := context.TODO()
	err = h.Update(ctx, newItem, oldItem)
	assert.Equal(t, resource.ErrNotFound, err)

	err = h.Insert(ctx, []*resource.Item{oldItem})
	assert.NoError(t, err)
	err = h.Update(ctx, newItem, oldItem)
	assert.NoError(t, err)

	// Update refused if original item's etag doesn't match stored one
	err = h.Update(ctx, newItem, oldItem)
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
			"id":  "1234",
			"foo": "bar",
		},
	}

	// Can't delete a non existing item
	ctx := context.TODO()
	err = h.Delete(ctx, item)
	assert.Equal(t, resource.ErrNotFound, err)

	err = h.Insert(ctx, []*resource.Item{item})
	assert.NoError(t, err)
	err = h.Delete(ctx, item)
	assert.NoError(t, err)

	// Update refused if original item's etag doesn't match stored one
	err = h.Insert(ctx, []*resource.Item{item})
	assert.NoError(t, err)
	item.ETag = "etag2"
	err = h.Delete(ctx, item)
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
	defer cleanup(c, "testclear")()
	h := NewHandler(c, "testclear", "test")
	items := []*resource.Item{
		{ID: "1", Payload: map[string]interface{}{"id": "1", "name": "a"}},
		{ID: "2", Payload: map[string]interface{}{"id": "2", "name": "b"}},
		{ID: "3", Payload: map[string]interface{}{"id": "3", "name": "c"}},
		{ID: "4", Payload: map[string]interface{}{"id": "4", "name": "d"}},
	}

	ctx := context.TODO()
	err = h.Insert(ctx, items)
	assert.NoError(t, err)

	q, err := query.New("", `{name:{$in:["c","d"]}}`, "", nil)
	if assert.NoError(t, err) {
		deleted, err := h.Clear(ctx, q)
		assert.NoError(t, err)
		assert.Equal(t, 2, deleted)
	}

	q, err = query.New("", `{id:"2"}`, "", nil)
	if assert.NoError(t, err) {
		deleted, err := h.Clear(ctx, q)
		assert.NoError(t, err)
		assert.Equal(t, 1, deleted)
	}
}

func TestFind(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	c, err := elastic.NewClient()
	if !assert.NoError(t, err) {
		return
	}
	defer cleanup(c, "testfind")()
	h := NewHandler(c, "testfind", "test")
	h.Refresh = "true"
	h2 := NewHandler(c, "testfind", "test2")
	h2.Refresh = "true"
	items := []*resource.Item{
		{ID: "1", Payload: map[string]interface{}{"id": "1", "name": "a", "age": 1}},
		{ID: "2", Payload: map[string]interface{}{"id": "2", "name": "b", "age": 2}},
		{ID: "3", Payload: map[string]interface{}{"id": "3", "name": "c", "age": 3}},
		{ID: "4", Payload: map[string]interface{}{"id": "4", "name": "d", "age": 4}},
	}
	ctx := context.TODO()
	assert.NoError(t, h.Insert(ctx, items))
	assert.NoError(t, h2.Insert(ctx, items))

	q, err := query.New("", "", "", nil)
	if assert.NoError(t, err) {
		l, err := h.Find(ctx, q)
		if assert.NoError(t, err) {
			assert.Equal(t, 4, l.Total)
			assert.Len(t, l.Items, 4)
			// Do not check result's content as its order is unpredictable
		}
	}

	q, err = query.New("", `{name:"c"}`, "", query.Page(1, 100, 0))
	if assert.NoError(t, err) {
		l, err := h.Find(ctx, q)
		if assert.NoError(t, err) {
			assert.Equal(t, 1, l.Total)
			if assert.Len(t, l.Items, 1) {
				item := l.Items[0]
				assert.Equal(t, "3", item.ID)
				assert.Equal(t, map[string]interface{}{"id": "3", "name": "c", "age": float64(3)}, item.Payload)
			}
		}
	}

	// FIXME $in and $nin are broken with "Fielddata is disabled on text fields by default." error.
	// q, err = query.New("", `{name:{$in:["c","d"]}}`, "name", query.Page(1, 100, 0))
	// if assert.NoError(t, err) {
	// 	l, err := h.Find(ctx, q)
	// 	if assert.NoError(t, err) {
	// 		assert.Equal(t, 2, l.Total)
	// 		if assert.Len(t, l.Items, 2) {
	// 			item := l.Items[0]
	// 			assert.Equal(t, "3", item.ID)
	// 			assert.Equal(t, map[string]interface{}{"id": "3", "name": "c", "age": float64(3)}, item.Payload)
	// 			item = l.Items[1]
	// 			assert.Equal(t, "4", item.ID)
	// 			assert.Equal(t, map[string]interface{}{"id": "4", "name": "d", "age": float64(4)}, item.Payload)
	// 		}
	// 	}
	// }

	// q, err = query.New("", `{name:{$nin:["c","d"]}}`, "name", query.Page(1, 100, 0))
	// if assert.NoError(t, err) {
	// 	l, err := h.Find(ctx, q)
	// 	if assert.NoError(t, err) {
	// 		assert.Equal(t, 2, l.Total)
	// 		if assert.Len(t, l.Items, 2) {
	// 			item := l.Items[0]
	// 			assert.Equal(t, "3", item.ID)
	// 			assert.Equal(t, map[string]interface{}{"id": "3", "name": "c", "age": float64(3)}, item.Payload)
	// 			item = l.Items[1]
	// 			assert.Equal(t, "4", item.ID)
	// 			assert.Equal(t, map[string]interface{}{"id": "4", "name": "d", "age": float64(4)}, item.Payload)
	// 		}
	// 	}
	// }

	q, err = query.New("", `{id:"3"}`, "", query.Page(1, 1, 0))
	if assert.NoError(t, err) {
		l, err := h.Find(ctx, q)
		if assert.NoError(t, err) {
			assert.Equal(t, 1, l.Total)
			if assert.Len(t, l.Items, 1) {
				item := l.Items[0]
				assert.Equal(t, "3", item.ID)
				assert.Equal(t, map[string]interface{}{"id": "3", "name": "c", "age": float64(3)}, item.Payload)
			}
		}
	}

	q, err = query.New("", `{id:"10"}`, "", query.Page(1, 1, 0))
	if assert.NoError(t, err) {
		l, err := h.Find(ctx, q)
		if assert.NoError(t, err) {
			assert.Equal(t, 0, l.Total)
			assert.Len(t, l.Items, 0)
		}
	}

	q, err = query.New("", `{id:{$in:["3","4","10"]}}`, "", nil)
	if assert.NoError(t, err) {
		l, err := h.Find(ctx, q)
		if assert.NoError(t, err) {
			assert.Equal(t, 2, l.Total)
			assert.Len(t, l.Items, 2)
		}
	}
}

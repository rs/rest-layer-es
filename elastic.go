// Package es is a REST Layer resource storage handler for ElasticSearch
package es

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v3"
)

// Handler handles resource storage in an ElasticSearch index.
type Handler struct {
	client *elastic.Client
	index  string
	typ    string
}

// NewHandler creates an new ElasticSearch storage handler for the given index/type
func NewHandler(client *elastic.Client, index, typ string) *Handler {
	return &Handler{
		client: client,
		index:  index,
		typ:    typ,
	}
}

// Insert inserts new items in the ElasticSearch index
func (m *Handler) Insert(ctx context.Context, items []*resource.Item) error {
	bulk := m.client.Bulk()
	for _, item := range items {
		id, ok := item.ID.(string)
		if !ok {
			return errors.New("non string IDs are not supported with ElasticSearch")
		}
		doc := buildDoc(item)
		req := elastic.NewBulkIndexRequest().OpType("create").Index(m.index).Type(m.typ).Id(id).Doc(doc)
		bulk.Add(req)
	}
	// Apply context deadline if any
	if t := ctxTimeout(ctx); t != "" {
		bulk.Timeout(t)
	}
	res, err := bulk.Do()
	if err != nil {
		if !translateError(&err) {
			err = fmt.Errorf("insert error: %v", err)
		}
	} else if res.Errors {
		for i, f := range res.Failed() {
			// CAVEAT on a bulk insert, if some items are in error, the operation is not atomic
			// and the request will partially succeed. I don't see how to perform atomic bulk insert
			// with ES.
			if f.Error.Type == "document_already_exists_exception" {
				err = resource.ErrConflict
			} else {
				err = fmt.Errorf("insert error on item #%d: %#v", i+1, f.Error)
			}
			break
		}
	}
	return err
}

// Elastic Search provides it's own concurrency update mecanism using numerical versioning incompatible with
// REST layer's etag system. To bridge the two, we first get the document, ensures the etag is valid and
// use the ES document's version to perform a conditional update. This function encapsulate this check and
// return either an error or the document version.
func (m *Handler) validateEtag(id, etag string) (int64, error) {
	res, err := m.client.Get().Index(m.index).Type(m.typ).Id(id).FetchSource(false).Fields(etagField).Do()
	if err != nil {
		if !translateError(&err) {
			err = fmt.Errorf("etag check error: %v", err)
		}
		return 0, err
	}
	if f, ok := res.Fields[etagField].([]interface{}); ok && res.Version != nil && len(f) == 1 && f[0] == etag {
		return *res.Version, nil
	}
	return 0, resource.ErrConflict
}

// Update replace an item by a new one in the ElasticSearch index
func (m *Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	id, ok := original.ID.(string)
	if !ok {
		return errors.New("non string IDs are not supported with ElasticSearch")
	}
	ver, err := m.validateEtag(id, original.ETag)
	if err != nil {
		return err
	}
	// Check if context is still valid
	if ctx.Err() != nil {
		return ctx.Err()
	}
	doc := buildDoc(item)
	u := m.client.Update().Index(m.index).Type(m.typ)
	// Apply context deadline if any
	if t := ctxTimeout(ctx); t != "" {
		u.Timeout(t)
	}
	_, err = u.Id(id).Doc(doc).Version(ver).Do()
	if err != nil {
		if !translateError(&err) {
			err = fmt.Errorf("update error: %v", err)
		}
	}
	return err
}

// Delete deletes an item from the ElasticSearch index
func (m *Handler) Delete(ctx context.Context, item *resource.Item) error {
	id, ok := item.ID.(string)
	if !ok {
		return errors.New("non string IDs are not supported with ElasticSearch")
	}
	ver, err := m.validateEtag(id, item.ETag)
	if err != nil {
		return err
	}
	// Check if context is still valid
	if ctx.Err() != nil {
		return ctx.Err()
	}
	d := m.client.Delete().Index(m.index).Type(m.typ)
	// Apply context deadline if any
	if t := ctxTimeout(ctx); t != "" {
		d.Timeout(t)
	}
	_, err = d.Id(id).Version(ver).Do()
	if err != nil {
		if !translateError(&err) {
			err = fmt.Errorf("delete error: %v", err)
		}
	}
	return err
}

// Clear clears all items from the ElasticSearch index matching the lookup
func (m *Handler) Clear(ctx context.Context, lookup *resource.Lookup) (int, error) {
	return 0, resource.ErrNotImplemented
}

// Find items from the ElasticSearch index matching the provided lookup
func (m *Handler) Find(ctx context.Context, lookup *resource.Lookup, page, perPage int) (*resource.ItemList, error) {
	// When query pattern is a single document request by its id, use the ES GET API
	if q := lookup.Filter(); len(q) == 1 && page == 1 && perPage == 1 {
		if eq, ok := q[0].(schema.Equal); ok && eq.Field == "id" {
			if id, ok := eq.Value.(string); ok {
				return m.get(ctx, id)
			}
		}
	}

	s := m.client.Search().Index(m.index).Type(m.typ)

	// Apply context deadline if any
	if t := ctxTimeout(ctx); t != "" {
		s.Timeout(t)
	}

	// Apply query
	q, err := getQuery(lookup)
	if err != nil {
		return nil, fmt.Errorf("find query tranlation error (index=%s, type=%s): %v", m.index, m.typ, err)
	}
	if q != nil {
		s.Query(q)
	}

	// Apply sort
	if sf := getSort(lookup); len(sf) > 0 {
		s.SortBy(sf...)
	}

	// Apply pagination
	if perPage >= 0 {
		s.From(page).Size(perPage)
	}

	// Perform query
	res, err := s.Do()
	// Translate some generic errors
	if err != nil {
		if elastic.IsTimeout(err) {
			err = context.DeadlineExceeded
		}
		return nil, fmt.Errorf("find error (index=%s, type=%s): %v", m.index, m.typ, err)
	}

	// Fetch the result and return it as a resource.ItemList
	list := &resource.ItemList{Page: page, Total: 0, Items: []*resource.Item{}}
	if res.Hits == nil {
		return list, nil
	}

	list.Total = int(res.Hits.TotalHits)
	for i, hit := range res.Hits.Hits {
		d := map[string]interface{}{}
		err := json.Unmarshal(*hit.Source, &d)
		if err != nil {
			return nil, fmt.Errorf("find unmarshaling error for item #%d: %v", i+1, err)
		}
		list.Items = append(list.Items, buildItem(hit.Id, d))
	}

	return list, nil
}

// get uses the ES GET API to retrieve a single document by its id instead of performing a
// slower search.
func (m *Handler) get(ctx context.Context, id string) (*resource.ItemList, error) {
	res, err := m.client.Get().Index(m.index).Type(m.typ).Id(id).Do()
	if err != nil && !elastic.IsNotFound(err) {
		return nil, fmt.Errorf("find item error (index=%s, type=%s, id=%s): %v", m.index, m.typ, id, err)
	}
	list := &resource.ItemList{Page: 1, Total: 0, Items: []*resource.Item{}}
	if elastic.IsNotFound(err) {
		return list, nil
	}
	d := map[string]interface{}{}
	if err = json.Unmarshal(*res.Source, &d); err != nil {
		return nil, fmt.Errorf("find item unmarshaling error: %v", err)
	}
	list.Items = append(list.Items, buildItem(id, d))
	return list, nil
}

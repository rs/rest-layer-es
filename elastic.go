// Package es is a REST Layer resource storage handler for ElasticSearch
package es

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rs/rest-layer/resource"
	"gopkg.in/olivere/elastic.v3"
)

// Handler handles resource storage in an ElasticSearch index.
type Handler struct {
	client *elastic.Client
	index  string
	typ    string
	// Refresh sets the refresh flag to true on all write operation to ensure
	// writes are reflected into search results immediately after the operation.
	// Setting this parameter to true has performance impacts.
	Refresh bool
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
func (h *Handler) Insert(ctx context.Context, items []*resource.Item) error {
	bulk := h.client.Bulk()
	for _, item := range items {
		id, ok := item.ID.(string)
		if !ok {
			return errors.New("non string IDs are not supported with ElasticSearch")
		}
		doc := buildDoc(item)
		req := elastic.NewBulkIndexRequest().OpType("create").Index(h.index).Type(h.typ).Id(id).Doc(doc)
		bulk.Add(req)
	}
	// Apply context deadline if any
	if t := ctxTimeout(ctx); t != "" {
		bulk.Timeout(t)
	}
	// Set the refresh flag to true if requested
	bulk.Refresh(h.Refresh)
	res, err := bulk.DoC(ctx)
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
func (h *Handler) validateEtag(ctx context.Context, id, etag string) (int64, error) {
	res, err := h.client.Get().Index(h.index).Type(h.typ).Id(id).FetchSource(false).Fields(etagField).DoC(ctx)
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
func (h *Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	id, ok := original.ID.(string)
	if !ok {
		return errors.New("non string IDs are not supported with ElasticSearch")
	}
	ver, err := h.validateEtag(ctx, id, original.ETag)
	if err != nil {
		return err
	}
	// Check if context is still valid
	if ctx.Err() != nil {
		return ctx.Err()
	}
	doc := buildDoc(item)
	u := h.client.Update().Index(h.index).Type(h.typ)
	// Set the refresh flag to true if requested
	u.Refresh(h.Refresh)
	// Apply context deadline if any
	if t := ctxTimeout(ctx); t != "" {
		u.Timeout(t)
	}
	_, err = u.Id(id).Doc(doc).Version(ver).DoC(ctx)
	if err != nil {
		if !translateError(&err) {
			err = fmt.Errorf("update error: %v", err)
		}
	}
	return err
}

// Delete deletes an item from the ElasticSearch index
func (h *Handler) Delete(ctx context.Context, item *resource.Item) error {
	id, ok := item.ID.(string)
	if !ok {
		return errors.New("non string IDs are not supported with ElasticSearch")
	}
	ver, err := h.validateEtag(ctx, id, item.ETag)
	if err != nil {
		return err
	}
	// Check if context is still valid
	if ctx.Err() != nil {
		return ctx.Err()
	}
	d := h.client.Delete().Index(h.index).Type(h.typ)
	// Apply context deadline if any
	if t := ctxTimeout(ctx); t != "" {
		d.Timeout(t)
	}
	// Set the refresh flag to true if requested
	d.Refresh(h.Refresh)
	_, err = d.Id(id).Version(ver).DoC(ctx)
	if err != nil {
		if !translateError(&err) {
			err = fmt.Errorf("delete error: %v", err)
		}
	}
	return err
}

// Clear clears all items from the ElasticSearch index matching the lookup
func (h *Handler) Clear(ctx context.Context, lookup *resource.Lookup) (int, error) {
	return 0, resource.ErrNotImplemented
}

// Find items from the ElasticSearch index matching the provided lookup
func (h *Handler) Find(ctx context.Context, lookup *resource.Lookup, offset, limit int) (*resource.ItemList, error) {
	s := h.client.Search().Index(h.index).Type(h.typ)

	// Apply context deadline if any
	if t := ctxTimeout(ctx); t != "" {
		s.Timeout(t)
	}

	// Apply query
	q, err := getQuery(lookup)
	if err != nil {
		return nil, fmt.Errorf("find query tranlation error (index=%s, type=%s): %v", h.index, h.typ, err)
	}
	if q != nil {
		s.Query(q)
	}

	// Apply sort
	if sf := getSort(lookup); len(sf) > 0 {
		s.SortBy(sf...)
	}

	// Apply pagination
	if offset > 0 {
		s.From(offset)
	}
	if limit >= 0 {
		s.Size(limit)
	}

	// Perform query
	res, err := s.DoC(ctx)
	// Translate some generic errors
	if err != nil {
		if !translateError(&err) {
			err = fmt.Errorf("find error (index=%s, type=%s): %v", h.index, h.typ, err)
		}
		return nil, err
	}

	// Fetch the result and return it as a resource.ItemList
	list := &resource.ItemList{Total: 0, Items: []*resource.Item{}}
	if res.Hits == nil || res.Hits.TotalHits == 0 {
		return list, nil
	}

	list.Total = int(res.Hits.TotalHits)
	list.Items = make([]*resource.Item, len(res.Hits.Hits))
	for i, hit := range res.Hits.Hits {
		d := map[string]interface{}{}
		err := json.Unmarshal(*hit.Source, &d)
		if err != nil {
			return nil, fmt.Errorf("find unmarshaling error for item #%d: %v", i+1, err)
		}
		list.Items[i] = buildItem(hit.Id, d)
	}

	return list, nil
}

// MultiGet implements the optional MultiGetter interface
func (h *Handler) MultiGet(ctx context.Context, ids []interface{}) ([]*resource.Item, error) {
	g := h.client.MultiGet()

	// Add item ids to retreive
	for _, v := range ids {
		id, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("non string IDs are not supported with ElasticSearch (index=%s, type=%s, id=%#v)",
				h.index, h.typ, v)
		}
		g.Add(elastic.NewMultiGetItem().Index(h.index).Type(h.typ).Id(id))
	}

	res, err := g.DoC(ctx)

	if err != nil {
		if !translateError(&err) {
			err = fmt.Errorf("multi get error (index=%s, type=%s, ids=%s): %v", h.index, h.typ, ids, err)
		}
		return nil, err
	}

	total := 0
	for _, subRes := range res.Docs {
		if subRes.Found {
			total++
		}
	}
	items := make([]*resource.Item, total)
	for i, subRes := range res.Docs {
		if !subRes.Found {
			continue
		}
		d := map[string]interface{}{}
		if err = json.Unmarshal(*subRes.Source, &d); err != nil {
			return nil, fmt.Errorf("multi get unmarshaling error (index=%s, type=%s, id=%s): %v", h.index, h.typ, subRes.Id, err)
		}
		items[i] = buildItem(subRes.Id, d)
	}
	return items, nil
}

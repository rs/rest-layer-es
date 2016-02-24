// Package es is a REST Layer resource storage handler for ElasticSearch
package es

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

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

func buildDoc(i *resource.Item) map[string]interface{} {
	d := map[string]interface{}{}
	for k, v := range i.Payload {
		d[k] = v
	}
	d["_etag"] = i.ETag
	d["_updated"] = i.Updated
	return d
}

func buildItem(id string, d map[string]interface{}) *resource.Item {
	i := resource.Item{
		ID:      id,
		Payload: map[string]interface{}{},
	}
	if etag, ok := d["_etag"].(string); ok {
		i.ETag = etag
	}
	if updated, ok := d["_updated"].(time.Time); ok {
		i.Updated = updated
	}
	for k, v := range d {
		if k != "_etag" && k != "_updated" {
			i.Payload[k] = v
		}
	}
	return &i
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
	_, err := bulk.Do()
	// TODO check individual errors
	if err != nil {
		err = fmt.Errorf("insert error: %v", err)
	}
	return err
}

// Update replace an item by a new one in the ElasticSearch index
func (m *Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	id, ok := item.ID.(string)
	if !ok {
		return errors.New("non string IDs are not supported with ElasticSearch")
	}
	doc := buildDoc(item)
	script := elastic.NewScriptInline(`ctx._source._etag == etag && (ctx._source = doc)`)
	script.Lang("groovy")
	script.Param("etag", item.ETag)
	script.Param("doc", doc)
	_, err := m.client.Update().Index(m.index).Type(m.typ).Id(id).Script(script).Do()
	if err != nil {
		err = fmt.Errorf("update error: %v", err)
	}
	return err
}

// Delete deletes an item from the ElasticSearch index
func (m *Handler) Delete(ctx context.Context, item *resource.Item) error {
	return resource.ErrNotImplemented
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

	q, err := getQuery(lookup)
	if err != nil {
		return nil, fmt.Errorf("find query tranlation error (index=%s, type=%s): %v", m.index, m.typ, err)
	}
	if q != nil {
		s.Query(q)
	}

	if sf := getSort(lookup); len(sf) > 0 {
		s.SortBy(sf...)
	}

	if perPage >= 0 {
		s.From(page).Size(perPage)
	}

	res, err := s.Do()
	if err != nil {
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

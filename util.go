package es

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
	"gopkg.in/olivere/elastic.v5"
)

const (
	etagField    = "_etag"
	updatedField = "_updated"
)

// buildDoc builds an ElasticSearch document from a resource.Item
func buildDoc(i *resource.Item) map[string]interface{} {
	// Filter out id from the payload so we don't store it twice
	d := map[string]interface{}{}
	for k, v := range i.Payload {
		if k != "id" {
			d[k] = v
		}
	}
	if i.ETag != "" {
		d[etagField] = i.ETag
	}
	if !i.Updated.IsZero() {
		d[updatedField] = i.Updated
	}
	return d
}

// buildItem builds a resource.Item from an ElasticSearch document
func buildItem(id string, d map[string]interface{}) *resource.Item {
	i := resource.Item{
		ID:      id,
		Payload: map[string]interface{}{"id": id},
	}
	if etag, ok := d[etagField].(string); ok {
		i.ETag = etag
	}
	if updated, ok := d[updatedField].(time.Time); ok {
		i.Updated = updated
	}
	for k, v := range d {
		if k != etagField && k != updatedField {
			i.Payload[k] = v
		}
	}
	return &i
}

func isConflict(err interface{}) bool {
	if elastic.IsConflict(err) {
		return true
	}
	if e, ok := err.(*elastic.ErrorDetails); ok {
		return e.Type == "version_conflict_engine_exception"
	}
	return false
}

// translateError translates some generic errors to REST Layer errors
func translateError(err *error) bool {
	if elastic.IsTimeout(*err) {
		*err = context.DeadlineExceeded
		return true
	} else if isConflict(*err) {
		*err = resource.ErrConflict
		return true
	} else if elastic.IsNotFound(*err) {
		*err = resource.ErrNotFound
		return true
	}
	return false
}

// ctxTimeout returns an ES compatible timeout argument if context has a deadline
func ctxTimeout(ctx context.Context) string {
	if dl, ok := ctx.Deadline(); ok {
		dur := dl.Sub(time.Now())
		if dur < 0 {
			dur = 0
		}
		return fmt.Sprintf("%dms", int(dur/time.Millisecond))
	}
	return ""
}

func valuesToInterface(v []query.Value) []interface{} {
	I := make([]interface{}, len(v))
	for i, _v := range v {
		I[i] = _v
	}
	return I
}

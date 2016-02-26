package es

import (
	"fmt"
	"net/http"
	"time"

	"github.com/rs/rest-layer/resource"
	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v3"
)

const (
	etagField    = "_etag"
	updatedField = "_updated"
)

// buildDoc builds an ElasticSearch document from a resource.Item
func buildDoc(i *resource.Item) map[string]interface{} {
	d := map[string]interface{}{}
	for k, v := range i.Payload {
		d[k] = v
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
		Payload: map[string]interface{}{},
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
	if e, ok := err.(*elastic.Error); ok {
		return e.Status == http.StatusConflict
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

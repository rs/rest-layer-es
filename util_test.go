package es

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/rs/rest-layer/resource"
	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v3"
)

func TestBuildDoc(t *testing.T) {
	assert.Equal(t, map[string]interface{}{}, buildDoc(&resource.Item{}))
	assert.Equal(t, map[string]interface{}{"foo": "bar"},
		buildDoc(&resource.Item{Payload: map[string]interface{}{"foo": "bar"}}))
	assert.Equal(t, map[string]interface{}{"foo": "bar", "_etag": "123"},
		buildDoc(&resource.Item{Payload: map[string]interface{}{"id": "1", "foo": "bar"}, ETag: "123"}))
	assert.Equal(t, map[string]interface{}{"foo": "bar", "_updated": now},
		buildDoc(&resource.Item{Payload: map[string]interface{}{"id": "1", "foo": "bar"}, Updated: now}))
}

func TestBuildItem(t *testing.T) {
	assert.Equal(t, &resource.Item{ID: "1", Payload: map[string]interface{}{"id": "1"}},
		buildItem("1", map[string]interface{}{}))
	assert.Equal(t, &resource.Item{ID: "1", Payload: map[string]interface{}{"id": "1", "foo": "bar"}},
		buildItem("1", map[string]interface{}{"foo": "bar"}))
	assert.Equal(t, &resource.Item{ID: "1", ETag: "123", Payload: map[string]interface{}{"id": "1", "foo": "bar"}},
		buildItem("1", map[string]interface{}{"foo": "bar", "_etag": "123"}))
	assert.Equal(t, &resource.Item{ID: "1", Updated: now, Payload: map[string]interface{}{"id": "1", "foo": "bar"}},
		buildItem("1", map[string]interface{}{"foo": "bar", "_updated": now}))
}

func TestTranslateError(t *testing.T) {
	var err error

	err = errors.New("test")
	assert.False(t, translateError(&err))
	assert.Error(t, err, "test")

	err = &elastic.Error{Status: http.StatusRequestTimeout}
	assert.True(t, translateError(&err))
	assert.Equal(t, context.DeadlineExceeded, err)

	err = &elastic.Error{Status: http.StatusConflict}
	assert.True(t, translateError(&err))
	assert.Equal(t, resource.ErrConflict, err)

	err = &elastic.Error{Status: http.StatusNotFound}
	assert.True(t, translateError(&err))
	assert.Equal(t, resource.ErrNotFound, err)
}

func TestCtxTimeout(t *testing.T) {
	ctx := context.Background()
	assert.Equal(t, "", ctxTimeout(ctx))
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	assert.Equal(t, "999ms", ctxTimeout(ctx))
	ctx, _ = context.WithTimeout(context.Background(), 10*time.Millisecond)
	assert.Equal(t, "9ms", ctxTimeout(ctx))
	ctx, _ = context.WithTimeout(context.Background(), -1*time.Second)
	assert.Equal(t, "0ms", ctxTimeout(ctx))
}

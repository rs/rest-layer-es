package es

import (
	"testing"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/stretchr/testify/assert"
)

type UnsupportedExpression struct{}

func (u UnsupportedExpression) Match(p map[string]interface{}) bool {
	return false
}

func callGetQuery(q schema.Query) (interface{}, error) {
	l := resource.NewLookup()
	l.AddQuery(q)
	Q, err := getQuery(l)
	if err != nil {
		return nil, err
	}
	return Q.Source()
}

func callGetSort(s string, v schema.Validator) []interface{} {
	l := resource.NewLookup()
	l.SetSort(s, v)
	ss := []interface{}{}
	for _, sf := range getSort(l) {
		i, _ := sf.Source()
		ss = append(ss, i)
	}
	return ss
}

func TestGetQuery(t *testing.T) {
	var s interface{}
	var err error
	s, err = callGetQuery(schema.Query{schema.Equal{Field: "id", Value: "foo"}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"term": map[string]interface{}{
			"_id": "foo",
		},
	}, s)
	s, err = callGetQuery(schema.Query{schema.Equal{Field: "f", Value: "foo"}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"term": map[string]interface{}{
			"f": "foo",
		},
	}, s)
	s, err = callGetQuery(schema.Query{schema.NotEqual{Field: "f", Value: "foo"}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"bool": map[string]interface{}{
			"must_not": map[string]interface{}{
				"term": map[string]interface{}{
					"f": "foo",
				},
			},
		},
	}, s)
	s, err = callGetQuery(schema.Query{schema.GreaterThan{Field: "f", Value: 1}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"range": map[string]interface{}{
			"f": map[string]interface{}{
				"from":          float64(1),
				"to":            nil,
				"include_lower": false,
				"include_upper": true,
			},
		},
	}, s)
	s, err = callGetQuery(schema.Query{schema.GreaterOrEqual{Field: "f", Value: 1}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"range": map[string]interface{}{
			"f": map[string]interface{}{
				"from":          float64(1),
				"to":            nil,
				"include_lower": true,
				"include_upper": true,
			},
		},
	}, s)
	s, err = callGetQuery(schema.Query{schema.LowerThan{Field: "f", Value: 1}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"range": map[string]interface{}{
			"f": map[string]interface{}{
				"from":          nil,
				"to":            float64(1),
				"include_lower": true,
				"include_upper": false,
			},
		},
	}, s)
	s, err = callGetQuery(schema.Query{schema.LowerOrEqual{Field: "f", Value: 1}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"range": map[string]interface{}{
			"f": map[string]interface{}{
				"from":          nil,
				"to":            float64(1),
				"include_lower": true,
				"include_upper": true,
			},
		},
	}, s)
	s, err = callGetQuery(schema.Query{schema.In{Field: "f", Values: []schema.Value{"foo", "bar"}}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"terms": map[string]interface{}{
			"f": []interface{}{"foo", "bar"},
		},
	}, s)
	s, err = callGetQuery(schema.Query{schema.NotIn{Field: "f", Values: []schema.Value{"foo", "bar"}}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"bool": map[string]interface{}{
			"must_not": map[string]interface{}{
				"terms": map[string]interface{}{
					"f": []interface{}{"foo", "bar"},
				},
			},
		},
	}, s)
	s, err = callGetQuery(schema.Query{schema.And{schema.Equal{Field: "f", Value: "foo"}, schema.Equal{Field: "f", Value: "bar"}}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"bool": map[string]interface{}{
			"must": []interface{}{
				map[string]interface{}{
					"term": map[string]interface{}{
						"f": "foo",
					},
				},
				map[string]interface{}{
					"term": map[string]interface{}{
						"f": "bar",
					},
				},
			},
		},
	}, s)
	s, err = callGetQuery(schema.Query{schema.Or{schema.Equal{Field: "f", Value: "foo"}, schema.Equal{Field: "f", Value: "bar"}}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{
		"bool": map[string]interface{}{
			"should": []interface{}{
				map[string]interface{}{
					"term": map[string]interface{}{
						"f": "foo",
					},
				},
				map[string]interface{}{
					"term": map[string]interface{}{
						"f": "bar",
					},
				},
			},
		},
	}, s)
}

func TestGetQueryInvalid(t *testing.T) {
	var err error
	_, err = callGetQuery(schema.Query{UnsupportedExpression{}})
	assert.Equal(t, resource.ErrNotImplemented, err)
	_, err = callGetQuery(schema.Query{schema.And{UnsupportedExpression{}}})
	assert.Equal(t, resource.ErrNotImplemented, err)
	_, err = callGetQuery(schema.Query{schema.Or{UnsupportedExpression{}}})
	assert.Equal(t, resource.ErrNotImplemented, err)
}

func TestGetSort(t *testing.T) {
	var s []interface{}
	v := schema.Schema{Fields: schema.Fields{"id": schema.IDField, "f": {Sortable: true}}}
	s = callGetSort("", v)
	assert.Equal(t, []interface{}{}, s)
	s = callGetSort("id", v)
	assert.Equal(t, []interface{}{map[string]interface{}{"_id": map[string]interface{}{"order": "asc"}}}, s)
	s = callGetSort("f", v)
	assert.Equal(t, []interface{}{map[string]interface{}{"f": map[string]interface{}{"order": "asc"}}}, s)
	s = callGetSort("-f", v)
	assert.Equal(t, []interface{}{map[string]interface{}{"f": map[string]interface{}{"order": "desc"}}}, s)
	s = callGetSort("f,-f", v)
	assert.Equal(t, []interface{}{
		map[string]interface{}{"f": map[string]interface{}{"order": "asc"}},
		map[string]interface{}{"f": map[string]interface{}{"order": "desc"}}}, s)
}

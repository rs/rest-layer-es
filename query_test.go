package es

import (
	"reflect"
	"testing"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/schema/query"
	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v5"
)

type UnsupportedExpression struct{}

func (u UnsupportedExpression) Match(p map[string]interface{}) bool {
	return false
}

func (u UnsupportedExpression) Validate(v schema.Validator) error {
	return nil
}

func (u UnsupportedExpression) String() string {
	return ""
}

func TestGetQuery(t *testing.T) {
	cases := []struct {
		predicate string
		err       error
		want      elastic.Query
	}{
		{`{id:"foo"}`, nil,
			elastic.NewTermQuery("_id", "foo")},
		{`{f:"foo"}`, nil,
			elastic.NewTermQuery("f.keyword", "foo")},
		{`{f:{$ne:"foo"}}`, nil,
			elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("f.keyword", "foo"))},
		{`{f:{$gt:1}}`, nil,
			elastic.NewRangeQuery("f").From(float64(1)).IncludeLower(false).IncludeUpper(true)},
		{`{f:{$gte:1}}`, nil,
			elastic.NewRangeQuery("f").From(float64(1)).IncludeLower(true).IncludeUpper(true)},
		{`{f:{$lt:1}}`, nil,
			elastic.NewRangeQuery("f").To(float64(1)).IncludeLower(true).IncludeUpper(false)},
		{`{f:{$lte:1}}`, nil,
			elastic.NewRangeQuery("f").To(float64(1)).IncludeLower(true).IncludeUpper(true)},
		{`{f:{$in:["foo","bar"]}}`, nil,
			elastic.NewTermsQuery("f.keyword", "foo", "bar")},
		{`{f:{$nin:["foo","bar"]}}`, nil,
			elastic.NewBoolQuery().MustNot(elastic.NewTermsQuery("f.keyword", "foo", "bar"))},
		{`{f:{$regex:"fo[o]{1}.+is.+some"}}`, resource.ErrNotImplemented,
			nil},
		{`{$and:[{f:"foo"},{f:"bar"}]}`, nil,
			elastic.NewBoolQuery().Must(elastic.NewTermQuery("f.keyword", "foo"), elastic.NewTermQuery("f.keyword", "bar"))},
		{`{$or:[{f:"foo"},{f:"bar"}]}`, nil,
			elastic.NewBoolQuery().Should(elastic.NewTermQuery("f.keyword", "foo"), elastic.NewTermQuery("f.keyword", "bar"))},
	}
	for i := range cases {
		tc := cases[i]
		t.Run(tc.predicate, func(t *testing.T) {
			q, err := query.New("", tc.predicate, "", nil)
			if err != nil {
				t.Error(err)
			}
			got, err := getQuery(q)
			if !reflect.DeepEqual(err, tc.err) {
				t.Errorf("translatePredicate error:\ngot:  %v\nwant: %v", err, tc.err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("translatePredicate:\ngot:  %#v\nwant: %#v", got, tc.want)
			}
		})
	}
}

func TestTranslatePredicateInvalid(t *testing.T) {
	var err error
	_, err = translatePredicate(query.Predicate{UnsupportedExpression{}})
	assert.Equal(t, resource.ErrNotImplemented, err)
	_, err = translatePredicate(query.Predicate{query.And{UnsupportedExpression{}}})
	assert.Equal(t, resource.ErrNotImplemented, err)
	_, err = translatePredicate(query.Predicate{query.Or{UnsupportedExpression{}}})
	assert.Equal(t, resource.ErrNotImplemented, err)
}

func TestGetSort(t *testing.T) {
	var s []elastic.Sorter
	s = getSort(&query.Query{Sort: query.Sort{}})
	assert.Equal(t, []elastic.Sorter(nil), s)
	s = getSort(&query.Query{Sort: query.Sort{{Name: "id"}}})
	assert.Equal(t, []elastic.Sorter{elastic.NewFieldSort(getField("id", true)).Asc()}, s)
	s = getSort(&query.Query{Sort: query.Sort{{Name: "f"}}})
	assert.Equal(t, []elastic.Sorter{elastic.NewFieldSort(getField("f", true)).Asc()}, s)
	s = getSort(&query.Query{Sort: query.Sort{{Name: "f", Reversed: true}}})
	assert.Equal(t, []elastic.Sorter{elastic.NewFieldSort(getField("f", true)).Desc()}, s)
	s = getSort(&query.Query{Sort: query.Sort{{Name: "f"}, {Name: "f", Reversed: true}}})
	assert.Equal(t, []elastic.Sorter{
		elastic.NewFieldSort(getField("f", true)).Asc(),
		elastic.NewFieldSort(getField("f", true)).Desc(),
	}, s)
}

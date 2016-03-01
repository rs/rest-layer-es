package es

import (
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"gopkg.in/olivere/elastic.v3"
)

// getField translate a schema field into a ES field:
//
//  - id -> _id with in order to tape on the ES _id key
func getField(f string) string {
	if f == "id" {
		return "_id"
	}
	return f
}

// getQuery transform a resource.Lookup into a ES query
func getQuery(l *resource.Lookup) (elastic.Query, error) {
	qs, err := translateQuery(l.Filter())
	if err != nil {
		return nil, err
	}
	switch len(qs) {
	case 0:
		return nil, nil
	case 1:
		// If a single root query is returned, do not wrap
		return qs[0], nil
	default:
		bq := elastic.NewBoolQuery()
		bq.Must(qs...)
		return bq, nil
	}
}

// getSort transform a resource.Lookup into an ES sort list.
func getSort(l *resource.Lookup) []elastic.Sorter {
	ln := len(l.Sort())
	if ln == 0 {
		return nil
	}
	s := make([]elastic.Sorter, ln)
	for i, sort := range l.Sort() {
		if len(sort) > 0 && sort[0] == '-' {
			s[i] = elastic.NewFieldSort(getField(sort[1:])).Desc()
		} else {
			s[i] = elastic.NewFieldSort(getField(sort)).Asc()
		}
	}
	return s
}

func translateQuery(q schema.Query) ([]elastic.Query, error) {
	qs := []elastic.Query{}
	for _, exp := range q {
		switch t := exp.(type) {
		case schema.And:
			and := elastic.NewBoolQuery()
			for _, subExp := range t {
				sq, err := translateQuery(schema.Query{subExp})
				if err != nil {
					return nil, err
				}
				and.Must(sq...)
			}
			qs = append(qs, and)
		case schema.Or:
			or := elastic.NewBoolQuery()
			for _, subExp := range t {
				sq, err := translateQuery(schema.Query{subExp})
				if err != nil {
					return nil, err
				}
				or.Should(sq...)
			}
			qs = append(qs, or)
		case schema.In:
			qs = append(qs, elastic.NewTermsQuery(getField(t.Field), valuesToInterface(t.Values)...))
		case schema.NotIn:
			b := elastic.NewBoolQuery()
			b.MustNot(elastic.NewTermsQuery(getField(t.Field), valuesToInterface(t.Values)...))
			qs = append(qs, b)
		case schema.Equal:
			qs = append(qs, elastic.NewTermQuery(getField(t.Field), t.Value))
		case schema.NotEqual:
			b := elastic.NewBoolQuery()
			b.MustNot(elastic.NewTermQuery(getField(t.Field), t.Value))
			qs = append(qs, b)
		case schema.GreaterThan:
			r := elastic.NewRangeQuery(getField(t.Field))
			r.Gt(t.Value)
			qs = append(qs, r)
		case schema.GreaterOrEqual:
			r := elastic.NewRangeQuery(getField(t.Field))
			r.Gte(t.Value)
			qs = append(qs, r)
		case schema.LowerThan:
			r := elastic.NewRangeQuery(getField(t.Field))
			r.Lt(t.Value)
			qs = append(qs, r)
		case schema.LowerOrEqual:
			r := elastic.NewRangeQuery(getField(t.Field))
			r.Lte(t.Value)
			qs = append(qs, r)
		default:
			return nil, resource.ErrNotImplemented
		}
	}
	return qs, nil
}

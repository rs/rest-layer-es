package es

import (
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
	"gopkg.in/olivere/elastic.v5"
)

// getField translate a schema field into a ES field:
//
//  - id -> _id with in order to tape on the ES _id key
//  - keyword=true -> appends .keyword to the field name
func getField(f string, keyword bool) string {
	if f == "id" {
		return "_id"
	} else if keyword {
		return f + ".keyword"
	}
	return f
}

// getQuery transform a resource.Lookup into a ES query
func getQuery(q *query.Query) (elastic.Query, error) {
	qs, err := translatePredicate(q.Predicate)
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
func getSort(q *query.Query) []elastic.Sorter {
	if len(q.Sort) == 0 {
		return nil
	}
	s := make([]elastic.Sorter, len(q.Sort))
	for i, sort := range q.Sort {
		if sort.Reversed {
			s[i] = elastic.NewFieldSort(getField(sort.Name, true)).Desc()
		} else {
			s[i] = elastic.NewFieldSort(getField(sort.Name, true)).Asc()
		}
	}
	return s
}

func translatePredicate(q query.Predicate) ([]elastic.Query, error) {
	qs := []elastic.Query{}
	for _, exp := range q {
		switch t := exp.(type) {
		case *query.And:
			and := elastic.NewBoolQuery()
			for _, subExp := range *t {
				sq, err := translatePredicate(query.Predicate{subExp})
				if err != nil {
					return nil, err
				}
				and.Must(sq...)
			}
			qs = append(qs, and)
		case *query.Or:
			or := elastic.NewBoolQuery()
			for _, subExp := range *t {
				sq, err := translatePredicate(query.Predicate{subExp})
				if err != nil {
					return nil, err
				}
				or.Should(sq...)
			}
			qs = append(qs, or)
		case *query.In:
			qs = append(qs, elastic.NewTermsQuery(getField(t.Field, true), valuesToInterface(t.Values)...))
		case *query.NotIn:
			b := elastic.NewBoolQuery()
			b.MustNot(elastic.NewTermsQuery(getField(t.Field, true), valuesToInterface(t.Values)...))
			qs = append(qs, b)
		case *query.Equal:
			qs = append(qs, elastic.NewTermQuery(getField(t.Field, true), t.Value))
		case *query.NotEqual:
			b := elastic.NewBoolQuery()
			b.MustNot(elastic.NewTermQuery(getField(t.Field, true), t.Value))
			qs = append(qs, b)
		case *query.GreaterThan:
			r := elastic.NewRangeQuery(getField(t.Field, false)).Gt(t.Value)
			qs = append(qs, r)
		case *query.GreaterOrEqual:
			r := elastic.NewRangeQuery(getField(t.Field, false)).Gte(t.Value)
			qs = append(qs, r)
		case *query.LowerThan:
			r := elastic.NewRangeQuery(getField(t.Field, false)).Lt(t.Value)
			qs = append(qs, r)
		case *query.LowerOrEqual:
			r := elastic.NewRangeQuery(getField(t.Field, false)).Lte(t.Value)
			qs = append(qs, r)
		default:
			return nil, resource.ErrNotImplemented
		}
	}
	return qs, nil
}

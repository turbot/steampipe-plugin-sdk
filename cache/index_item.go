package cache

import (
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// IndexBucket contains index items for all cache results for a given table and qual set
type IndexBucket struct {
	Items []*IndexItem
}

func newIndexBucket() *IndexBucket {
	return &IndexBucket{}
}

func (b *IndexBucket) Append(item *IndexItem) *IndexBucket {
	b.Items = append(b.Items, item)
	return b
}

// Get finds an index item which satisfies all columns
func (b *IndexBucket) Get(qualMap map[string]*proto.Quals, columns []string, limit int64) *IndexItem {
	for _, item := range b.Items {
		if item.SatisfiesQuals(qualMap) && item.SatisfiesColumns(columns) && item.SatisfiesLimit(limit) {
			return item
		}
	}
	return nil
}

// IndexItem stores the columns and cached index for a single cached query result
// note - this index item it tied to a specific table and set of quals
type IndexItem struct {
	Columns []string
	Key     string
	Limit   int64
	Quals   map[string]*proto.Quals
}

func NewIndexItem(columns []string, key string, limit int64, quals map[string]*proto.Quals) *IndexItem {
	return &IndexItem{
		Columns: columns,
		Key:     key,
		Limit:   limit,
		Quals:   quals,
	}
}

// SatisfiesColumns returns whether this index item satisfies the given columns
// used when determining whether this IndexItem satisfies a cache reques
func (i IndexItem) SatisfiesColumns(columns []string) bool {
	for _, c := range columns {
		if !helpers.StringSliceContains(i.Columns, c) {
			log.Printf("[TRACE] SatisfiesColumns returning false - %s missing from %s", c, strings.Join(columns, ","))
			return false
		}
	}
	return true
}

// SatisfiesLimit returns whether this index item satisfies the given limit
// used when determining whether this IndexItem satisfies a cache reques
func (i IndexItem) SatisfiesLimit(limit int64) bool {
	// if index item has is no limit, it will be -1
	if i.Limit == -1 {
		log.Printf("[TRACE] SatisfiesLimit limit %d, no item limit - satisfied", limit)
		return true
	}
	log.Printf("[TRACE] SatisfiesLimit limit %d, item limit %d ", limit, i.Limit)
	// if 'limit' is -1 and i.Limit is not, we cannot satisfy this
	if limit == -1 {
		return false
	}
	// otherwise just check whether limit is <= item limit>
	res := limit <= i.Limit
	log.Printf("[TRACE] satisfied = %v", res)
	return res

}

// SatisfiesQuals
//check quals must be MORE restrictive the our quals
//   i.e. our quals must be a subset of check quals
//   eg
//our quals [], check quals [id="1"] 				-> SATISFIED
//our quals [id="1"], check quals [id="1"] 		-> SATISFIED
//our quals [id="1"], check quals [id="1", foo=2] -> SATISFIED
//our quals [id="1", foo=2], check quals [id="1"] -> NOT SATISFIED
func (i IndexItem) SatisfiesQuals(checkQualMap map[string]*proto.Quals) bool {
	for col, indexQuals := range i.Quals {
		// if we have quals the passed in map does not, we DO NOT satisfy
		checkQuals, ok := checkQualMap[col]
		if !ok || !indexQuals.IsASubsetOf(checkQuals) {
			return false
		}
	}
	return true
}

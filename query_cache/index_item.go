package query_cache

import (
	"log"
	"strings"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// IndexItem stores the columns and cached index for a single cached query result
// note - this index item it tied to a specific table and set of quals
type IndexItem struct {
	Columns       []string
	Key           string
	Limit         int64
	SortOrder     []*proto.SortColumn
	Quals         map[string]*proto.Quals
	InsertionTime time.Time
	PageCount     int64
}

func NewIndexItem(req *CacheRequest) *IndexItem {
	return &IndexItem{
		Columns:       req.Columns,
		Key:           req.resultKeyRoot,
		Limit:         req.Limit,
		Quals:         req.QualMap,
		SortOrder:     req.SortOrder,
		InsertionTime: time.Now(),
		PageCount:     req.pageCount,
	}
}

func (i IndexItem) satisfiesRequest(columns []string, limit int64, qualMap map[string]*proto.Quals, sortOrder []*proto.SortColumn, keyColumns map[string]*proto.KeyColumn) bool {
	satisfiedColumns := i.satisfiesColumns(columns)
	satisfiesLimit := i.satisfiesLimit(limit)
	satisfiesQuals := i.satisfiesQuals(qualMap, keyColumns)
	satisfiesSortOrder := i.satisfiesSortOrder(sortOrder)

	log.Printf("[TRACE] IndexItem satisfiesRequest: satisfiedColumns %v satisfiesLimit %v satisfiesQuals %v satisfiesSortOrder %v", satisfiedColumns, satisfiesLimit, satisfiesQuals, satisfiesSortOrder)
	return satisfiedColumns && satisfiesLimit && satisfiesQuals && satisfiesSortOrder
}

func (i IndexItem) satisfiedByRequest(req *CacheRequest, keyColumns map[string]*proto.KeyColumn) bool {
	// make an index item for the request
	requestIndexItem := NewIndexItem(req)
	return requestIndexItem.satisfiesRequest(i.Columns, i.Limit, i.Quals, i.SortOrder, keyColumns)
}

// satisfiesColumns returns whether this index item satisfies the given columns
// used when determining whether this IndexItem satisfies a cache reques
func (i IndexItem) satisfiesColumns(columns []string) bool {
	for _, c := range columns {
		if !helpers.StringSliceContains(i.Columns, c) {
			log.Printf("[TRACE] satisfiesColumns returning false - %s missing from %s", c, strings.Join(i.Columns, ","))
			return false
		}
	}
	return true
}

// satisfiesLimit returns whether this index item satisfies the given limit
// used when determining whether this IndexItem satisfies a cache reques
func (i IndexItem) satisfiesLimit(limit int64) bool {
	// if index item has is no limit, it will be -1
	if i.Limit == -1 {
		log.Printf("[TRACE] satisfiesLimit limit %d, no item limit - satisfied", limit)
		return true
	}
	log.Printf("[TRACE] satisfiesLimit limit %d, item limit %d ", limit, i.Limit)
	// if 'limit' is -1 and i.Limit is not, we cannot satisfy this
	if limit == -1 {
		return false
	}
	// otherwise just check whether limit is <= item limit>
	res := limit <= i.Limit
	log.Printf("[TRACE] satisfied = %v", res)
	return res

}

// satisfiesQuals
// does this index item satisfy the check quals
// all data returned by check quals is returned by index quals
//
//	i.e. check quals must be a 'subset' of index quals
//	eg
//	   our quals [], check quals [id="1"] 				-> SATISFIED
//	   our quals [id="1"], check quals [id="1"] 		-> SATISFIED
//	   our quals [id="1"], check quals [id="1", foo=2] -> SATISFIED
//	   our quals [id="1", foo=2], check quals [id="1"] -> NOT SATISFIED
//
// NOTE: some columns cannot use this subset logic. Generally this applies to columns which represent a filter which
// is executed server side to filter the data returned.
// In this case, we only identify a cache hit if the cached data has the _same_ value for the given colummn
//
// NOTE: if the IndexItem has a limit, the quals must be IDENTICAL (ignoring ordering)
func (i IndexItem) satisfiesQuals(checkQualMap map[string]*proto.Quals, keyColumns map[string]*proto.KeyColumn) bool {
	qualsString := grpc.QualMapToLogLine(i.Quals)

	log.Printf("[TRACE] satisfiesQuals, limit %d, columns %v, quals %s", i.Limit, i.Columns, qualsString)

	if i.Limit != -1 {
		res := grpc.QualMapsEqual(checkQualMap, i.Quals)
		log.Printf("[TRACE] there is a limit so quals must be identical to satisfy - result: %v", res)
		return res
	}

	for col, indexQuals := range i.Quals {
		// if we have quals the check quals do not, we DO NOT satisfy
		checkQuals, ok := checkQualMap[col]
		var isSubset bool
		if ok {
			// isSubset means all data returned by check quals is returned by index quals
			isSubset = checkQuals.IsASubsetOf(indexQuals)
			log.Printf("[TRACE] satisfiesQuals index item has quals for %s which check quals also have - check if our quals for this colummn are a subset of the check quals", col)
			log.Printf("[TRACE] indexQuals %+v, checkQuals %+v, isSubset %v", indexQuals, checkQuals, isSubset)
		} else {
			log.Printf("[TRACE] satisfiesQuals index item has qual for %s which check quals do not - NOT SATISFIED", col)
		}
		log.Printf("[TRACE] get check qual %v, isSubset %v", ok, isSubset)
		if !ok || !isSubset {
			log.Printf("[TRACE] satisfiesQuals FALSE, limit %d, columns %v, quals %s", i.Limit, i.Columns, qualsString)
			return false
		}
	}

	// now for each of the check quals, see whether it requires an exact match in the cached data.
	// i.e. the same qual must exist in the cached data
	for col, checkQuals := range checkQualMap {
		if keyColumn, ok := keyColumns[col]; ok && keyColumn.CacheMatch == CacheMatchExact {
			quals, ok := i.Quals[col]
			if !ok || !quals.Equals(checkQuals) {
				log.Printf("[TRACE] satisfiesQuals FALSE, same qual does not exist in cached data - limit %d, columns %v, quals %s", i.Limit, i.Columns, qualsString)
				return false
			}
		}
	}
	log.Printf("[TRACE] satisfiesQuals TRUE, limit %d, columns %v, quals %s", i.Limit, i.Columns, qualsString)
	return true
}

// satisfiesTtl
// does this index item satisfy the ttl requirement
func (i IndexItem) satisfiesTtl(ttlSeconds int64) bool {
	timeSince := time.Since(i.InsertionTime)
	if timeSince > time.Duration(ttlSeconds)*time.Second {
		log.Printf("[TRACE] satisfiesTtl: cache ttl %d has expired (%fs)", ttlSeconds, timeSince.Seconds())
		return false
	}
	log.Printf("[TRACE] satisfiesTtl: cache ttl %d has NOT expired (%fs)", ttlSeconds, timeSince.Seconds())

	return true
}

// does this item satisfy the sort order
// if an order was specified, this item must implement the exact same order
func (i IndexItem) satisfiesSortOrder(sortOrder []*proto.SortColumn) bool {
	if len(sortOrder) != len(i.SortOrder) {
		return false
	}
	for idx, o := range sortOrder {
		if !o.Equals(i.SortOrder[idx]) {
			return false
		}
	}
	return true
}

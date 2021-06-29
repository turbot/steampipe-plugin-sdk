package plugin

import (
	"log"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// is there a single '=' qual for this column
func singleEqualsQual(column string, qualMap map[string]*proto.Quals) (*proto.Qual, bool) {
	quals, ok := qualMap[column]
	if !ok {
		return nil, false
	}

	if len(quals.Quals) == 1 && quals.Quals[0].GetStringValue() == "=" && quals.Quals[0].Value != nil {
		return quals.Quals[0], true
	}
	return nil, false
}

// look in a column-qual map for quals with column and operator matching the key column
func getMatchingQuals(keyColumn *KeyColumn, qualMap map[string]*proto.Quals) []*proto.Qual {
	log.Printf("[WARN] getMatchingQuals keyColumn %s qualMap %s", keyColumn, qualMap)

	quals, ok := qualMap[keyColumn.Column]
	if !ok {
		log.Printf("[WARN] getMatchingQuals returning false - qualMap does not contain any quals for colums %s", keyColumn.Column)
		return nil
	}

	var res []*proto.Qual
	for _, q := range quals.Quals {
		operator := q.GetStringValue()
		if helpers.StringSliceContains(keyColumn.Operators, operator) {
			res = append(res, q)

		}
	}
	if len(res) > 0 {
		log.Printf("[WARN] getMatchingQuals found %d quals matching key column %s", len(res), keyColumn)
	} else {
		log.Printf("[WARN] getMatchingQuals returning false - qualMap does not contain any matching quals for quals for key column %s", keyColumn)
	}

	return res
}

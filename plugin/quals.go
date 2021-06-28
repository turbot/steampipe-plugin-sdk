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

// look in a column-qual map for a qual with column and operator matching the key column
func qualExists(keyColumn *KeyColumn, qualMap map[string]*proto.Quals) (*proto.Qual, bool) {
	log.Printf("[WARN] qualExists keyColumn %s qualMap %s", keyColumn, qualMap)

	quals, ok := qualMap[keyColumn.Name]
	if !ok {
		log.Printf("[WARN] qualExists returning false - qualMap does not contain any quals for colums %s", keyColumn.Name)
		return nil, false
	}

	for _, q := range quals.Quals {
		operator := q.GetStringValue()
		if helpers.StringSliceContains(keyColumn.Operators, operator) {
			log.Printf("[WARN] qualExists found quals matching key column %s - operator %s", keyColumn, operator)
			return q, true
		}
	}

	log.Printf("[WARN] qualExists returning false - qualMap does not contain any matching quals for quals for key column %s", keyColumn)

	return nil, false
}

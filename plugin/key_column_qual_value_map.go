package plugin

import (
	"fmt"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v4/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
)

// KeyColumnEqualsQualMap is a map of column name to qual value, used to represent a map of any equals quals
type KeyColumnEqualsQualMap map[string]*proto.QualValue

func (m KeyColumnEqualsQualMap) String() string {
	if len(m) == 0 {
		return "{}"
	}
	var strs []string
	for k, v := range m {
		strs = append(strs, fmt.Sprintf("%s = %v", k, grpc.GetQualValue(v)))
	}
	return strings.Join(strs, "\n")
}

// GetListQualValues returns a map of all qual values with a List value
func (m KeyColumnEqualsQualMap) GetListQualValues() map[string]*proto.QualValueList {
	res := make(map[string]*proto.QualValueList)
	for k, v := range m {
		if listValue := v.GetListValue(); listValue != nil {
			res[k] = listValue
		}
	}
	return res
}

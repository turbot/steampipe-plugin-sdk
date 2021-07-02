package plugin

import (
	"fmt"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
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

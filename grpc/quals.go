package grpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

const (
	IPv4 = "IPv4"
	IPv6 = "IPv6"
)

func QualMapToString(qualMap map[string]*proto.Quals, pretty bool) string {
	if len(qualMap) == 0 {
		return ""
	}
	divider := "----------------------------------------------------------------\n"
	var sb strings.Builder
	if pretty {
		sb.WriteString("\n")
		sb.WriteString(divider)
		defer sb.WriteString(divider)
	}

	for _, quals := range qualMap {
		var qb strings.Builder
		for _, q := range quals.GetQuals() {
			qb.WriteString(QualToString(q))
		}
		sb.WriteString(qb.String())
	}

	return sb.String()
}

func QualMapToJSONString(qualMap map[string]*proto.Quals) (string, error) {
	var res []map[string]interface{}
	if len(qualMap) == 0 {
		return "[]", nil
	}

	for _, quals := range qualMap {
		for _, q := range quals.GetQuals() {
			res = append(res, map[string]interface{}{
				"column":   q.FieldName,
				"operator": q.GetStringValue(),
				"value":    GetQualValue(q.Value),
			})

		}
	}
	writeBuffer := bytes.NewBufferString("")
	encoder := json.NewEncoder(writeBuffer)
	encoder.SetIndent("", " ")
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(res); err != nil {
		return "", err
	}

	return writeBuffer.String(), nil
}

func QualToString(q *proto.Qual) string {
	return fmt.Sprintf("Column: %s, Operator: '%s', Value: '%v'\n", q.FieldName, q.GetStringValue(), GetQualValue(q.Value))
}

func QualEquals(left *proto.Qual, right *proto.Qual) bool {
	return QualToString(left) == QualToString(right)
}

func GetQualValue(v *proto.QualValue) interface{} {
	var qv interface{}
	switch v := v.GetValue().(type) {
	case *proto.QualValue_InetValue:
		qv = v.InetValue.Cidr
	case *proto.QualValue_JsonbValue:
		qv = v.JsonbValue
	case *proto.QualValue_StringValue:
		qv = v.StringValue
	case *proto.QualValue_Int64Value:
		qv = v.Int64Value
	case *proto.QualValue_DoubleValue:
		qv = v.DoubleValue
	case *proto.QualValue_BoolValue:
		qv = v.BoolValue
	case *proto.QualValue_TimestampValue:
		ts := v.TimestampValue
		qv = time.Unix(ts.Seconds, int64(ts.Nanos))
	case *proto.QualValue_ListValue:
		var values []interface{}
		for _, l := range v.ListValue.Values {
			values = append(values, GetQualValue(l))
		}
		qv = values
	}
	return qv
}

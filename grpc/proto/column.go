package proto

import (
	"encoding/json"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func (x *Column) ValueToInterface() (any, error) {
	// extract x value as interface from protobuf message
	var val any
	if bytes := x.GetJsonValue(); bytes != nil {
		if err := json.Unmarshal(bytes, &val); err != nil {
			return nil, err
		}
	} else if timestamp := x.GetTimestampValue(); timestamp != nil {
		// convert from protobuf timestamp to a RFC 3339 time string
		val = ptypes.TimestampString(timestamp)
	} else {
		// get the first field descriptor and value (we only expect x message to contain a single field
		x.ProtoReflect().Range(func(descriptor protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			// is this value null?
			if descriptor.JSONName() == "nullValue" {
				val = nil
			} else {
				val = v.Interface()
			}
			return false
		})
	}

	return val, nil
}

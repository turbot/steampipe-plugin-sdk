package utils

import "reflect"

// https://mangatmodi.medium.com/go-check-nil-interface-the-right-way-d142776edef1
func InterfaceIsNil(i interface{}) bool {
	if i == nil {
		return true
	}
	switch reflect.TypeOf(i).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		return reflect.ValueOf(i).IsNil()
	}
	return false
}

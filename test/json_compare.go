package test

import (
	"encoding/json"
	"reflect"
)

func DeepJsonCompare(jsonA, jsonB []byte) bool {

	var objA, objB map[string]interface{}
	if err := json.Unmarshal(jsonA, &objA); err != nil {
		return false
	}
	if err := json.Unmarshal(jsonB, &objB); err != nil {
		return false
	}

	return reflect.DeepEqual(objA, objB)

}

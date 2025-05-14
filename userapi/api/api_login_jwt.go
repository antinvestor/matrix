// Copyright 2021 The Global.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// JWTClaims Create a struct that will be encoded to a JWT.
// We add jwt.StandardClaims as an embedded type, to provide fields like expiry time
type JWTClaims struct {
	Ext         map[string]any `json:"ext,omitempty"`
	TenantID    string         `json:"tenant_id,omitempty"`
	PartitionID string         `json:"partition_id,omitempty"`
	AccessID    string         `json:"access_id,omitempty"`
	ContactID   string         `json:"contact_id,omitempty"`
	DeviceID    string         `json:"device_id,omitempty"`
	Roles       []string       `json:"roles,omitempty"`
	jwt.RegisteredClaims
}

func (a *JWTClaims) GetTenantId() string {

	result := a.TenantID
	if result != "" {
		return result
	}
	val, ok := a.Ext["tenant_id"]
	if !ok {
		return ""
	}

	result, ok = val.(string)
	if !ok {
		return ""
	}

	return result
}

func (a *JWTClaims) GetPartitionId() string {

	result := a.PartitionID
	if result != "" {
		return result
	}
	val, ok := a.Ext["partition_id"]
	if !ok {
		return ""
	}

	result, ok = val.(string)
	if !ok {
		return ""
	}

	return result
}

func (a *JWTClaims) GetAccessId() string {

	result := a.AccessID
	if result != "" {
		return result
	}
	val, ok := a.Ext["access_id"]
	if !ok {
		return ""
	}

	result, ok = val.(string)
	if !ok {
		return ""
	}

	return result
}

func (a *JWTClaims) GetContactId() string {

	result := a.ContactID
	if result != "" {
		return result
	}
	val, ok := a.Ext["contact_id"]
	if !ok {
		return ""
	}

	result, ok = val.(string)
	if !ok {
		return ""
	}

	return result
}

func (a *JWTClaims) GetDeviceId() string {

	result := a.ContactID
	if result != "" {
		return result
	}
	val, ok := a.Ext["device_id"]
	if !ok {
		return ""
	}

	result, ok = val.(string)
	if !ok {
		return ""
	}

	return result
}

func (a *JWTClaims) GetRoles() []string {

	var result = a.Roles
	if len(result) > 0 {
		return result
	}

	roles, ok := a.Ext["roles"]
	if !ok {
		roles, ok = a.Ext["role"]
		if !ok {
			return result
		}
	}

	roleStr, ok2 := roles.(string)
	if ok2 {
		result = append(result, strings.Split(roleStr, ",")...)
	}

	return result
}

func (a *JWTClaims) ServiceName() string {

	result := ""
	val, ok := a.Ext["service_name"]
	if !ok {
		return ""
	}

	result, ok = val.(string)
	if !ok {
		return ""
	}

	return result
}

// AsMetadata Creates a string map to be used as metadata in queue data
func (a *JWTClaims) AsMetadata() map[string]string {

	m := make(map[string]string)
	m["sub"] = a.Subject
	m["tenant_id"] = a.GetTenantId()
	m["partition_id"] = a.GetPartitionId()
	m["access_id"] = a.GetAccessId()
	m["contact_id"] = a.GetContactId()
	m["device_id"] = a.GetDeviceId()
	m["roles"] = strings.Join(a.GetRoles(), ",")
	return m
}

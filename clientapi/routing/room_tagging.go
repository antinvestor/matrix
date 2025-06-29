// Copyright 2019 Sumukha PK
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

package routing

import (
	"encoding/json"
	"net/http"

	"github.com/antinvestor/gomatrix"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/httputil"
	"github.com/antinvestor/matrix/clientapi/producers"
	"github.com/antinvestor/matrix/userapi/api"
	"github.com/pitabwire/util"
)

// GetTags implements GET /_matrix/client/r0/user/{userID}/rooms/{roomID}/tags
func GetTags(
	req *http.Request,
	userAPI api.ClientUserAPI,
	device *api.Device,
	userID string,
	roomID string,
	syncProducer *producers.SyncAPIProducer,
) util.JSONResponse {

	if device.UserID != userID {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Cannot retrieve another user's tags"),
		}
	}

	tagContent, err := obtainSavedTags(req, userID, roomID, userAPI)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("obtainSavedTags failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: tagContent,
	}
}

// PutTag implements PUT /_matrix/client/r0/user/{userID}/rooms/{roomID}/tags/{tag}
// Put functionality works by getting existing data from the Cm (if any), adding
// the tag to the "map" and saving the new "map" to the Cm
func PutTag(
	req *http.Request,
	userAPI api.ClientUserAPI,
	device *api.Device,
	userID string,
	roomID string,
	tag string,
	syncProducer *producers.SyncAPIProducer,
) util.JSONResponse {

	if device.UserID != userID {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Cannot modify another user's tags"),
		}
	}

	var properties gomatrix.TagProperties
	if reqErr := httputil.UnmarshalJSONRequest(req, &properties); reqErr != nil {
		return *reqErr
	}

	tagContent, err := obtainSavedTags(req, userID, roomID, userAPI)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("obtainSavedTags failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if tagContent.Tags == nil {
		tagContent.Tags = make(map[string]gomatrix.TagProperties)
	}
	tagContent.Tags[tag] = properties

	if err = saveTagData(req, userID, roomID, userAPI, tagContent); err != nil {
		util.Log(req.Context()).WithError(err).Error("saveTagData failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// DeleteTag implements DELETE /_matrix/client/r0/user/{userID}/rooms/{roomID}/tags/{tag}
// Delete functionality works by obtaining the saved tags, removing the intended tag from
// the "map" and then saving the new "map" in the Cm
func DeleteTag(
	req *http.Request,
	userAPI api.ClientUserAPI,
	device *api.Device,
	userID string,
	roomID string,
	tag string,
	syncProducer *producers.SyncAPIProducer,
) util.JSONResponse {

	if device.UserID != userID {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Cannot modify another user's tags"),
		}
	}

	tagContent, err := obtainSavedTags(req, userID, roomID, userAPI)
	if err != nil {
		util.Log(req.Context()).WithError(err).Error("obtainSavedTags failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Check whether the tag to be deleted exists
	if _, ok := tagContent.Tags[tag]; ok {
		delete(tagContent.Tags, tag)
	} else {
		// Spec only defines 200 responses for this endpoint so we don't return anything else.
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}

	if err = saveTagData(req, userID, roomID, userAPI, tagContent); err != nil {
		util.Log(req.Context()).WithError(err).Error("saveTagData failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// obtainSavedTags gets all tags scoped to a userID and roomID
// from the database
func obtainSavedTags(
	req *http.Request,
	userID string,
	roomID string,
	userAPI api.ClientUserAPI,
) (tags gomatrix.TagContent, err error) {
	dataReq := api.QueryAccountDataRequest{
		UserID:   userID,
		RoomID:   roomID,
		DataType: "m.tag",
	}
	dataRes := api.QueryAccountDataResponse{}
	err = userAPI.QueryAccountData(req.Context(), &dataReq, &dataRes)
	if err != nil {
		return
	}
	data, ok := dataRes.RoomAccountData[roomID]["m.tag"]
	if !ok {
		return
	}
	if err = json.Unmarshal(data, &tags); err != nil {
		return
	}
	return tags, nil
}

// saveTagData saves the provided tag data into the database
func saveTagData(
	req *http.Request,
	userID string,
	roomID string,
	userAPI api.ClientUserAPI,
	Tag gomatrix.TagContent,
) error {
	newTagData, err := json.Marshal(Tag)
	if err != nil {
		return err
	}
	dataReq := api.InputAccountDataRequest{
		UserID:      userID,
		RoomID:      roomID,
		DataType:    "m.tag",
		AccountData: json.RawMessage(newTagData),
	}
	dataRes := api.InputAccountDataResponse{}
	return userAPI.InputAccountData(req.Context(), &dataReq, &dataRes)
}

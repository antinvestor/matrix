// Copyright 2017 Vector Creations Ltd
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

package distributed

import (
	"context"
	profilev1 "github.com/antinvestor/apis/go/profile/v1"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/clientapi/auth/authtypes"
	"github.com/antinvestor/matrix/userapi/storage/tables"
)

const (
	propertiesMatrixName = "matrix_name"
	propertiesOriginName = "name"
	propertiesAvaterUri  = "avatar_uri"
)

type profilesApi struct {
	serverNoticesLocalpart string
	profileClient          *profilev1.ProfileClient
}

func NewProfilesApi(ctx context.Context, profileClient *profilev1.ProfileClient) (tables.ProfileTable, error) {
	return &profilesApi{
		profileClient: profileClient,
	}, nil
}

func (s *profilesApi) toProfile(profileID string, properties map[string]string, contacts []*profilev1.ContactObject) *authtypes.Profile {

	name, ok := properties[propertiesMatrixName]
	if !ok {
		name = properties[propertiesOriginName]
	}

	var internalContacts []authtypes.Contact
	for _, contact := range contacts {
		internalContacts = append(internalContacts, s.toContact(contact))
	}

	return &authtypes.Profile{
		Localpart:   profileID,
		ServerName:  s.serverNoticesLocalpart,
		DisplayName: name,
		AvatarURL:   properties[propertiesAvaterUri],
		Contacts:    internalContacts,
	}
}

func (s *profilesApi) toContact(contact *profilev1.ContactObject) authtypes.Contact {
	return authtypes.Contact{
		ID:       contact.GetId(),
		Detail:   contact.GetDetail(),
		Type:     contact.GetType().String(),
		Verified: contact.GetVerified(),
	}
}

func (s *profilesApi) InsertProfile(
	_ context.Context,
	_ string, _ spec.ServerName,
) (err error) {
	return
}

func (s *profilesApi) SelectProfileByLocalpart(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (*authtypes.Profile, error) {

	pObj, err := s.profileClient.GetProfileByID(ctx, localpart)
	if err != nil {
		return nil, err
	}

	return s.toProfile(pObj.GetId(), pObj.GetProperties(), pObj.GetContacts()), nil
}

func (s *profilesApi) SetAvatarURL(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	avatarURL string,
) (*authtypes.Profile, bool, error) {

	updateResponse, err := s.profileClient.Client.Update(ctx, &profilev1.UpdateRequest{
		Id: localpart,
		Properties: map[string]string{
			"avatar_uri": avatarURL,
		},
		State: 0,
	})
	if err != nil {
		return nil, false, err
	}
	pObj := updateResponse.GetData()
	return s.toProfile(pObj.GetId(), pObj.GetProperties(), pObj.GetContacts()), true, err
}

func (s *profilesApi) SetDisplayName(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	displayName string,
) (*authtypes.Profile, bool, error) {

	updateResponse, err := s.profileClient.Client.Update(ctx, &profilev1.UpdateRequest{
		Id: localpart,
		Properties: map[string]string{
			"matrix_name": displayName,
		},
		State: 0,
	})
	if err != nil {
		return nil, false, err
	}
	pObj := updateResponse.GetData()
	return s.toProfile(pObj.GetId(), pObj.GetProperties(), pObj.GetContacts()), true, err
}

func (s *profilesApi) SelectProfilesBySearch(
	ctx context.Context, localPart, searchString string, limit int,
) ([]authtypes.Profile, error) {
	var profiles []authtypes.Profile

	roster, err := s.profileClient.Client.SearchRoster(ctx, &profilev1.SearchRosterRequest{
		ProfileId: localPart,
		Query:     searchString,
		Count:     int32(limit),
	})
	if err != nil {
		return nil, err
	}

	var response *profilev1.SearchRosterResponse
	for {
		response, err = roster.Recv()
		if err != nil {
			return profiles, err
		}

		for _, rosterObj := range response.GetData() {
			contact := []*profilev1.ContactObject{
				rosterObj.GetContact(),
			}
			profiles = append(profiles, *s.toProfile(rosterObj.GetId(), rosterObj.GetExtra(), contact))
		}

	}
}

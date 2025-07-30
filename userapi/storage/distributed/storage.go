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
	"fmt"

	devicev1 "github.com/antinvestor/apis/go/device/v1"
	profilev1 "github.com/antinvestor/apis/go/profile/v1"
	"github.com/antinvestor/matrix/userapi/storage/shared"
)

// NewDatabase creates a new accounts and profiles database
func NewDatabase(ctx context.Context, profileClient *profilev1.ProfileClient, deviceClient *devicev1.DeviceClient, localDb *shared.Database) (*shared.Database, error) {

	var err error
	localDb.Profiles, err = NewProfilesApi(ctx, profileClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create profiles api: %w", err)
	}

	localDb.Devices, err = NewDevicesApi(ctx, deviceClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create devices api: %w", err)
	}

	return localDb, nil

}

// Copyright 2020 The Matrix.org Foundation C.I.C.
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

// Package mscs implements Matrix Spec Changes from https://github.com/matrix-org/matrix-doc
package mscs

import (
	"context"

	"github.com/antinvestor/matrix/internal/caching"
	"github.com/antinvestor/matrix/internal/httputil"
	"github.com/antinvestor/matrix/internal/sqlutil"
	"github.com/antinvestor/matrix/setup"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/mscs/msc2836"
	"github.com/pitabwire/util"
	"github.com/sirupsen/logrus"
)

// Enable MSCs - returns an error on unknown MSCs
func Enable(ctx context.Context, cfg *config.Dendrite, cm *sqlutil.Connections, routers httputil.Routers, monolith *setup.Monolith, caches *caching.Caches) error {
	for _, msc := range cfg.MSCs.MSCs {
		util.GetLogger(ctx).WithField("msc", msc).Info("Enabling MSC")
		if err := EnableMSC(ctx, cfg, cm, routers, monolith, msc, caches); err != nil {
			return err
		}
	}
	return nil
}

func EnableMSC(ctx context.Context, cfg *config.Dendrite, cm *sqlutil.Connections, routers httputil.Routers, monolith *setup.Monolith, msc string, caches *caching.Caches) error {
	switch msc {
	case "msc2836":
		return msc2836.Enable(ctx, cfg, cm, routers, monolith.RoomserverAPI, monolith.FederationAPI, monolith.UserAPI, monolith.KeyRing)
	case "msc2444": // enabled inside federationapi
	case "msc2753": // enabled inside clientapi
	default:
		logrus.Warnf("EnableMSC: unknown MSC '%s', this MSC is either not supported or is natively supported by Dendrite", msc)
	}
	return nil
}

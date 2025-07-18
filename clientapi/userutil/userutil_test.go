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

package userutil

import (
	"testing"

	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/setup/config"
)

var (
	localpart                         = "somelocalpart"
	serverName        spec.ServerName = "someservername"
	invalidServerName spec.ServerName = "invalidservername"
	goodUserID                        = "@" + localpart + ":" + string(serverName)
	badUserID                         = "@bad:user:name@noservername:"
)

// TestGoodUserID checks that correct localpart is returned for a valid user ID.
func TestGoodUserID(t *testing.T) {
	cfg := &config.Global{
		SigningIdentity: fclient.SigningIdentity{
			ServerName: serverName,
		},
	}

	lp, _, err := ParseUsernameParam(goodUserID, cfg)

	if err != nil {
		t.Errorf("User ID Parsing failed for %s with error: %s", goodUserID, err.Error())
	}

	if lp != localpart {
		t.Errorf("Incorrect username, returned: %s should be: %s", lp, localpart)
	}
}

// TestWithLocalpartOnly checks that localpart is returned when usernameParam contains only localpart.
func TestWithLocalpartOnly(t *testing.T) {
	cfg := &config.Global{
		SigningIdentity: fclient.SigningIdentity{
			ServerName: serverName,
		},
	}

	lp, _, err := ParseUsernameParam(localpart, cfg)

	if err != nil {
		t.Error("User ID Parsing failed for ", localpart, " with error: ", err.Error())
	}

	if lp != localpart {
		t.Error("Incorrect username, returned: ", lp, " should be: ", localpart)
	}
}

// TestIncorrectDomain checks for error when there's server name mismatch.
func TestIncorrectDomain(t *testing.T) {
	cfg := &config.Global{
		SigningIdentity: fclient.SigningIdentity{
			ServerName: invalidServerName,
		},
	}

	_, _, err := ParseUsernameParam(goodUserID, cfg)

	if err == nil {
		t.Errorf("Invalid Domain should return an error")
	}
}

// TestBadUserID checks that ParseUsernameParam fails for invalid user ID
func TestBadUserID(t *testing.T) {
	cfg := &config.Global{
		SigningIdentity: fclient.SigningIdentity{
			ServerName: serverName,
		},
	}

	_, _, err := ParseUsernameParam(badUserID, cfg)

	if err == nil {
		t.Errorf("Illegal User ID should return an error")
	}
}

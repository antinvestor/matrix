// Copyright 2022 The Global.org Foundation C.I.C.
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

package test

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/roomserver/types"
)

type eventMods struct {
	originServerTS time.Time
	origin         spec.ServerName
	stateKey       *string
	unsigned       interface{}
	keyID          gomatrixserverlib.KeyID
	privKey        ed25519.PrivateKey
	authEvents     []string
}

type eventModifier func(e *eventMods)

func WithTimestamp(ts time.Time) eventModifier {
	return func(e *eventMods) {
		e.originServerTS = ts
	}
}

func WithStateKey(skey string) eventModifier {
	return func(e *eventMods) {
		e.stateKey = &skey
	}
}

func WithUnsigned(unsigned interface{}) eventModifier {
	return func(e *eventMods) {
		e.unsigned = unsigned
	}
}

func WithAuthIDs(evs []string) eventModifier {
	return func(e *eventMods) {
		e.authEvents = evs
	}
}

func WithKeyID(keyID gomatrixserverlib.KeyID) eventModifier {
	return func(e *eventMods) {
		e.keyID = keyID
	}
}

func WithPrivateKey(pkey ed25519.PrivateKey) eventModifier {
	return func(e *eventMods) {
		e.privKey = pkey
	}
}

func WithOrigin(origin spec.ServerName) eventModifier {
	return func(e *eventMods) {
		e.origin = origin
	}
}

// Reverse a list of events
func Reversed(in []*types.HeaderedEvent) []*types.HeaderedEvent {
	out := make([]*types.HeaderedEvent, len(in))
	for i := 0; i < len(in); i++ {
		out[i] = in[len(in)-i-1]
	}
	return out
}

func AssertEventIDsEqual(t *testing.T, gotEventIDs []string, wants []*types.HeaderedEvent) {
	t.Helper()
	if len(gotEventIDs) != len(wants) {
		t.Errorf("length mismatch: got %d events, want %d", len(gotEventIDs), len(wants))
		return
	}
	for i := range wants {
		w := wants[i].EventID()
		g := gotEventIDs[i]
		if w != g {
			t.Errorf("event at index %d mismatch:\ngot  %s\n\nwant %s", i, string(g), string(w))
		}
	}
}

func AssertEventsEqual(t *testing.T, gots, wants []*types.HeaderedEvent) {
	t.Helper()
	if len(gots) != len(wants) {
		t.Fatalf("length mismatch: got %d events, want %d", len(gots), len(wants))
	}
	for i := range wants {
		w := wants[i].JSON()
		g := gots[i].JSON()
		if !DeepJsonCompare(w, g) {
			t.Errorf("event at index %d mismatch:\ngot  %s\n\nwant %s", i, string(g), string(w))
		}
	}
}

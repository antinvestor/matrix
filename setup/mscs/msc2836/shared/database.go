// Copyright 2022 The Matrix.org Foundation C.I.C.
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

package shared

import (
	"context"
	"encoding/json"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/roomserver/types"
)

type EventInfo struct {
	EventID        string
	OriginServerTS spec.Timestamp
	RoomID         string
}

type Database interface {
	// StoreRelation stores the parent->child and child->parent relationship for later querying.
	// Also stores the event metadata e.g timestamp
	StoreRelation(ctx context.Context, ev *types.HeaderedEvent) error
	// ChildrenForParent returns the events who have the given `eventID` as an m.relationship with the
	// provided `relType`. The returned slice is sorted by origin_server_ts according to whether
	// `recentFirst` is true or false.
	ChildrenForParent(ctx context.Context, eventID, relType string, recentFirst bool) ([]EventInfo, error)
	// ParentForChild returns the parent event for the given child `eventID`. The eventInfo should be nil if
	// there is no parent for this child event, with no error. The parent eventInfo can be missing the
	// timestamp if the event is not known to the server.
	ParentForChild(ctx context.Context, eventID, relType string) (*EventInfo, error)
	// UpdateChildMetadata persists the children_count and children_hash from this event if and only if
	// the count is greater than what was previously there. If the count is updated, the event will be
	// updated to be unexplored.
	UpdateChildMetadata(ctx context.Context, ev *types.HeaderedEvent) error
	// ChildMetadata returns the children_count and children_hash for the event ID in question.
	// Also returns the `explored` flag, which is set to true when MarkChildrenExplored is called and is set
	// back to `false` when a larger count is inserted via UpdateChildMetadata.
	// Returns nil error if the event ID does not exist.
	ChildMetadata(ctx context.Context, eventID string) (count int, hash []byte, explored bool, err error)
	// MarkChildrenExplored sets the 'explored' flag on this event to `true`.
	MarkChildrenExplored(ctx context.Context, eventID string) error
}

func ParentChildEventIDs(ev *types.HeaderedEvent) (parent, child, relType string) {
	if ev == nil {
		return
	}
	body := struct {
		Relationship struct {
			RelType string `json:"rel_type"`
			EventID string `json:"event_id"`
		} `json:"m.relationship"`
	}{}
	if err := json.Unmarshal(ev.Content(), &body); err != nil {
		return
	}
	if body.Relationship.EventID == "" || body.Relationship.RelType == "" {
		return
	}
	return body.Relationship.EventID, ev.EventID(), body.Relationship.RelType
}

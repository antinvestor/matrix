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
	"context"
	"encoding/json"
	"sync"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/spec"
)

type InMemoryRelayDatabase struct {
	nid          int64
	nidMutex     sync.Mutex
	transactions map[int64]json.RawMessage
	associations map[spec.ServerName][]int64
}

func NewInMemoryRelayDatabase() *InMemoryRelayDatabase {
	return &InMemoryRelayDatabase{
		nid:          1,
		nidMutex:     sync.Mutex{},
		transactions: make(map[int64]json.RawMessage),
		associations: make(map[spec.ServerName][]int64),
	}
}

func (d *InMemoryRelayDatabase) InsertQueueEntry(
	_ context.Context,
	transactionID gomatrixserverlib.TransactionID,
	serverName spec.ServerName,
	nid int64,
) error {
	if _, ok := d.associations[serverName]; !ok {
		d.associations[serverName] = []int64{}
	}
	d.associations[serverName] = append(d.associations[serverName], nid)
	return nil
}

func (d *InMemoryRelayDatabase) DeleteQueueEntries(
	_ context.Context,
	serverName spec.ServerName,
	jsonNIDs []int64,
) error {
	for _, nid := range jsonNIDs {
		for index, associatedNID := range d.associations[serverName] {
			if associatedNID == nid {
				d.associations[serverName] = append(d.associations[serverName][:index], d.associations[serverName][index+1:]...)
			}
		}
	}

	return nil
}

func (d *InMemoryRelayDatabase) SelectQueueEntries(
	_ context.Context, serverName spec.ServerName,
	limit int,
) ([]int64, error) {
	results := []int64{}
	resultCount := limit
	if limit > len(d.associations[serverName]) {
		resultCount = len(d.associations[serverName])
	}
	if resultCount > 0 {
		for i := 0; i < resultCount; i++ {
			results = append(results, d.associations[serverName][i])
		}
	}

	return results, nil
}

func (d *InMemoryRelayDatabase) SelectQueueEntryCount(
	_ context.Context,
	serverName spec.ServerName,
) (int64, error) {
	return int64(len(d.associations[serverName])), nil
}

func (d *InMemoryRelayDatabase) InsertQueueJSON(
	_ context.Context,
	json string,
) (int64, error) {
	d.nidMutex.Lock()
	defer d.nidMutex.Unlock()

	nid := d.nid
	d.transactions[nid] = []byte(json)
	d.nid++

	return nid, nil
}

func (d *InMemoryRelayDatabase) DeleteQueueJSON(
	_ context.Context,
	nids []int64,
) error {
	for _, nid := range nids {
		delete(d.transactions, nid)
	}

	return nil
}

func (d *InMemoryRelayDatabase) SelectQueueJSON(
	_ context.Context,
	jsonNIDs []int64,
) (map[int64][]byte, error) {
	result := make(map[int64][]byte)
	for _, nid := range jsonNIDs {
		if transaction, ok := d.transactions[nid]; ok {
			result[nid] = transaction
		}
	}

	return result, nil
}

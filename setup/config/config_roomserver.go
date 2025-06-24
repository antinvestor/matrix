package config

import (
	"context"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/matrix/setup/constants"
	"github.com/pitabwire/util"
)

type RoomServer struct {
	Global *Global `yaml:"-"`

	DefaultRoomVersion gomatrixserverlib.RoomVersion `yaml:"default_room_version,omitempty"`

	Database DatabaseOptions `yaml:"database,omitempty"`

	Queues RoomServerQueues `yaml:"queues"`
}

func (c *RoomServer) Defaults(opts DefaultOpts) {
	c.DefaultRoomVersion = gomatrixserverlib.RoomVersionV10
	c.Database.Reference = "RoomServer"
	c.Database.Prefix = opts.RandomnessPrefix
	c.Database.DatabaseURI = opts.DSDatabaseConn
	c.Queues.Defaults(opts)
}

func (c *RoomServer) Verify(configErrs *Errors) {
	if c.Database.DatabaseURI == "" {
		checkNotEmpty(configErrs, "room_server.database.database_uri", string(c.Database.DatabaseURI))
	}

	if !gomatrixserverlib.KnownRoomVersion(c.DefaultRoomVersion) {
		configErrs.Add(fmt.Sprintf("invalid value for config key 'room_server.default_room_version': unsupported room version: %q", c.DefaultRoomVersion))
	} else if !gomatrixserverlib.StableRoomVersion(c.DefaultRoomVersion) {
		util.Log(context.TODO()).Warn("WARNING: Provided default room version %q is unstable", c.DefaultRoomVersion)
	}
}

type RoomServerQueues struct {
	// durable - InputRoomConsumer
	InputRoomEvent QueueOptions `yaml:"input_room_event"`
}

func (q *RoomServerQueues) Defaults(opts DefaultOpts) {

	q.InputRoomEvent = opts.defaultQ(constants.InputRoomEvent,
		KVOpt{K: "stream_retention", V: "interest"},
		KVOpt{K: "stream_subjects", V: fmt.Sprintf("%s.*", constants.InputRoomEvent)},
		KVOpt{K: "consumer_filter_subject", V: fmt.Sprintf("%s.*", constants.InputRoomEvent)},
		KVOpt{K: "consumer_headers_only", V: "true"},
		KVOpt{K: constants.QueueHeaderToExtendSubject, V: constants.RoomID})
}

func (q *RoomServerQueues) Verify(configErrs *Errors) {
	checkNotEmpty(configErrs, "room_server.queues.input_room_event", string(q.InputRoomEvent.DS))
}

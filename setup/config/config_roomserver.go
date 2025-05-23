package config

import (
	"fmt"
	"github.com/antinvestor/gomatrixserverlib"
	log "github.com/sirupsen/logrus"
)

type RoomServer struct {
	Global *Global `yaml:"-"`

	DefaultRoomVersion gomatrixserverlib.RoomVersion `yaml:"default_room_version,omitempty"`

	Database DatabaseOptions `yaml:"database,omitempty"`

	Queues RoomServerQueues `yaml:"queues"`
}

func (c *RoomServer) Defaults(opts DefaultOpts) {
	c.DefaultRoomVersion = gomatrixserverlib.RoomVersionV10
	c.Database.ConnectionString = opts.DatabaseConnectionStr
	c.Queues.Defaults(opts)
}

func (c *RoomServer) Verify(configErrs *ConfigErrors) {
	if c.Database.ConnectionString == "" {
		checkNotEmpty(configErrs, "room_server.database.connection_string", string(c.Database.ConnectionString))
	}

	if !gomatrixserverlib.KnownRoomVersion(c.DefaultRoomVersion) {
		configErrs.Add(fmt.Sprintf("invalid value for config key 'room_server.default_room_version': unsupported room version: %q", c.DefaultRoomVersion))
	} else if !gomatrixserverlib.StableRoomVersion(c.DefaultRoomVersion) {
		log.Warnf("WARNING: Provided default room version %q is unstable", c.DefaultRoomVersion)
	}
}

type RoomServerQueues struct {

	// durable - InputRoomConsumer
	InputRoomEvent QueueOptions `yaml:"input_room_event"`
}

func (q *RoomServerQueues) Defaults(opts DefaultOpts) {
	q.InputRoomEvent = QueueOptions{Prefix: opts.QueuePrefix, QReference: "RoomServerInputRoomConsumer", DS: "mem://RoomServerInputRoomEvent"}
}

func (q *RoomServerQueues) Verify(configErrs *ConfigErrors) {
	checkNotEmpty(configErrs, "room_server.queues.input_room_event", string(q.InputRoomEvent.DS))
}

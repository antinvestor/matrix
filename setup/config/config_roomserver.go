package config

import (
	"context"
	"fmt"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/pitabwire/util"

	"time"
)

type RoomServer struct {
	Global *Global `yaml:"-"`

	DefaultRoomVersion gomatrixserverlib.RoomVersion `yaml:"default_room_version,omitempty"`

	Database DatabaseOptions `yaml:"database,omitempty"`

	Queues RoomServerQueues `yaml:"queues"`

	// ActorSystem contains configuration for the Proto.Actor based distributed event processing system
	ActorSystem ActorConfig `yaml:"actor_system,omitempty"`
}

func (c *RoomServer) Defaults(opts DefaultOpts) {
	c.DefaultRoomVersion = gomatrixserverlib.RoomVersionV10
	c.Database.ConnectionString = opts.DSDatabaseConn
	c.Queues.Defaults(opts)

	c.ActorSystem.Defaults(opts)
}

func (c *RoomServer) Verify(configErrs *ConfigErrors) {
	if c.Database.ConnectionString == "" {
		checkNotEmpty(configErrs, "room_server.database.connection_string", string(c.Database.ConnectionString))
	}

	if !gomatrixserverlib.KnownRoomVersion(c.DefaultRoomVersion) {
		configErrs.Add(fmt.Sprintf("invalid value for config key 'room_server.default_room_version': unsupported room version: %q", c.DefaultRoomVersion))
	} else if !gomatrixserverlib.StableRoomVersion(c.DefaultRoomVersion) {
		util.Log(context.TODO()).Warn("WARNING: Provided default room version %q is unstable", c.DefaultRoomVersion)
	}

	c.ActorSystem.Verify(configErrs)
}

// ActorConfig contains configuration for the Proto.Actor system used for distributed event processing
type ActorConfig struct {
	// Enabled determines whether the actor system is enabled
	Enabled bool `yaml:"enabled"`

	// Host to use for the actor system
	Host string `yaml:"host"`

	// Port to use for the actor system
	Port int `yaml:"port"`

	// ClusterMode specifies the clustering mode: "kubernetes", "automanaged", or "none"
	ClusterMode string `yaml:"cluster_mode"`

	// ClusterSeeds is a list of cluster seed nodes in the format "host:port"
	ClusterSeeds []string `yaml:"cluster_seeds"`

	// IdleTimeout is the duration after which a room actor will stop if no messages are received
	IdleTimeout time.Duration `yaml:"idle_timeout"`
}

func (c *ActorConfig) Defaults(opts DefaultOpts) {
	// Default ActorSystem configuration
	c.Enabled = false // Disabled by default for backward compatibility
	c.Host = "localhost"
	c.Port = 8090
	c.ClusterMode = "none" // Default to no clustering
	c.ClusterSeeds = []string{"127.0.0.1:8090"}
	c.IdleTimeout = 1 * time.Minute

}

func (c *ActorConfig) Verify(configErrs *ConfigErrors) {
	// Only verify if the actor system is enabled
	if !c.Enabled {
		return
	}

	// Verify actor system configuration
	if c.Port <= 0 || c.Port > 65535 {
		configErrs.Add(fmt.Sprintf("invalid value for config key 'room_server.actor_system.port': %d, must be between 1 and 65535", c.Port))
	}

	// Verify cluster mode
	validModes := map[string]bool{"none": true, "automanaged": true, "kubernetes": true}
	if _, valid := validModes[c.ClusterMode]; !valid {
		configErrs.Add(fmt.Sprintf("invalid value for config key 'room_server.actor_system.cluster_mode': %s, must be one of 'none', 'automanaged', or 'kubernetes'", c.ClusterMode))
	}

	// Verify cluster seeds if using automanaged mode
	if c.ClusterMode == "automanaged" && len(c.ClusterSeeds) == 0 {
		configErrs.Add("missing value for config key 'room_server.actor_system.cluster_seeds' when using automanaged cluster mode")
	}

	// Verify idle timeout
	if c.IdleTimeout < 10*time.Second {
		configErrs.Add(fmt.Sprintf("invalid value for config key 'room_server.actor_system.idle_timeout': %s, must be at least 10 seconds", c.IdleTimeout))
	}
}

type RoomServerQueues struct {
	// durable - InputRoomConsumer
	InputRoomEvent QueueOptions `yaml:"input_room_event"`
}

func (q *RoomServerQueues) Defaults(opts DefaultOpts) {

	inputOpt := opts.defaultQ(InputRoomEvent)
	inputOpt.DS = inputOpt.DS.
		ExtendQuery("stream_subjects", fmt.Sprintf("%s.*", InputRoomEvent)).
		ExtendQuery("stream_name", InputRoomEvent).
		ExtendQuery("consumer_headers_only", "true").
		ExtendQuery("consumer_deliver_policy", "all").
		ExtendQuery("consumer_ack_policy", "explicit").
		ExtendQuery("consumer_replay_policy", "instant")

	q.InputRoomEvent = inputOpt
}

func (q *RoomServerQueues) Verify(configErrs *ConfigErrors) {
	checkNotEmpty(configErrs, "room_server.queues.input_room_event", string(q.InputRoomEvent.DS))
}

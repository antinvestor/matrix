package actor

import (
	"context"
	"errors"
	"github.com/antinvestor/gomatrixserverlib/spec"
	actorV1 "github.com/antinvestor/matrix/apis/actor/v1"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/config"
	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	cpk8s "github.com/asynkron/protoactor-go/cluster/clusterproviders/k8s"
	cptest "github.com/asynkron/protoactor-go/cluster/clusterproviders/test"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/disthash"
	"github.com/asynkron/protoactor-go/remote"
	"github.com/pitabwire/frame"
)

const (
	RoomActorQPrefix = "RoomActorQ_"
)

type HandlerFunc func(ctx context.Context, metadata map[string]string, message []byte) error

// RoomActorSystem manages a set of actors for processing room events
type RoomActorSystem struct {
	config      *config.ActorConfig
	qOpts       config.QueueOptions
	qm          queueutil.QueueManager
	actorSystem *actor.ActorSystem
	handlerFunc HandlerFunc
	clusterName string
	clusterKind string
	cluster     *cluster.Cluster
}

// NewRoomActorSystem creates a new room actor system
func NewRoomActorSystem(ctx context.Context, config *config.ActorConfig, qm queueutil.QueueManager, handlerFunc HandlerFunc) *RoomActorSystem {

	actorSystem := actor.NewActorSystem()

	actorSyst := &RoomActorSystem{
		config:      config,
		qm:          qm,
		actorSystem: actorSystem,
		handlerFunc: handlerFunc,
		clusterName: "matrix-cluster",
		clusterKind: "room-actors",
	}

	svc := frame.FromContext(ctx)
	svc.AddCleanupMethod(actorSyst.Shutdown)

	return actorSyst
}

// Start initializes the actor system
func (s *RoomActorSystem) Start(ctx context.Context) error {
	var err error

	// Configure the actor system for remote capability
	remoteConfig := remote.Configure(s.config.Host, 0)

	// Configuration for the cluster provider
	var clusterProvider cluster.ClusterProvider

	if s.config.ClusterMode == "kubernetes" {
		// Use Kubernetes provider
		clusterProvider, err = cpk8s.New()
		if err != nil {
			return err
		}
	} else {
		// Default to automanaged cluster
		clusterProvider = cptest.NewTestProvider(cptest.NewInMemAgent())
	}

	lookup := disthash.New()

	mb := actor.Unbounded(&RoomBehavior{})
	// Create the room actor props for the cluster
	roomActorProps := actor.PropsFromProducer(func() actor.Actor {
		return NewRoomActor(ctx, s.qm, s.handlerFunc)
	}, actor.WithMailbox(mb))

	// Register the room actor kind with the cluster
	roomKind := cluster.NewKind(s.clusterKind, roomActorProps)

	// Create the cluster configuration
	clusterConfig := cluster.Configure(
		s.clusterName,
		clusterProvider,
		lookup,
		remoteConfig,
		cluster.WithKinds(roomKind),
	)

	// Start the cluster
	s.cluster = cluster.New(s.actorSystem, clusterConfig)
	s.cluster.StartMember()
	return nil
}

// EnsureRoomActorExists creates or gets a room actor for the specified room
func (s *RoomActorSystem) EnsureRoomActorExists(_ context.Context, roomID *spec.RoomID, queueUri config.DataSource) error {

	message := &actorV1.SetupRoomRequest{
		RoomId:   roomID.String(),
		QueueUri: string(queueUri),
	}

	// Send the message to setup the room actor
	resp, err := s.cluster.Request(roomID.String(), s.clusterKind, message)
	if err != nil {
		return err
	}

	sResp := resp.(*actorV1.SetupRoomResponse)
	if !sResp.GetSuccess() {
		return errors.New(sResp.GetMessage())
	}

	return nil
}

// handleRoomActorStopped is called when a room actor stops
func (s *RoomActorSystem) handleRoomActorStopped(roomID string) {

}

// Receive handles messages sent to the actor system
func (s *RoomActorSystem) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actorV1.ActorStopped:
		s.handleRoomActorStopped(msg.GetRoomId())
	}
}

// Shutdown stops the actor system and all actors
func (s *RoomActorSystem) Shutdown(ctx context.Context) {
	if s.cluster != nil {
		s.cluster.Shutdown(true)
	}
	if s.actorSystem != nil && !s.actorSystem.IsStopped() {
		s.actorSystem.Shutdown()
	}
}

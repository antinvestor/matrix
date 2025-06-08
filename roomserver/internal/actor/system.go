package actor

import (
	"context"
	"fmt"
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
	"strconv"
	"time"
)

const (
	clusterManagerKind = "manager-actor"
	clusterManagerID   = "heimdal"

	clusterName = "matrix-room-cluster"
)

type HandlerFunc func(ctx context.Context, metadata map[string]string, message []byte) error

// Manager manages a set of actors for processing room events
type Manager struct {
	config      *config.ActorConfig
	qOpts       *config.QueueOptions
	qm          queueutil.QueueManager
	actorSystem *actor.ActorSystem

	handlerFunc HandlerFunc
	clusterName string
	cluster     *cluster.Cluster
}

// NewManager creates a new room actor system
func NewManager(ctx context.Context, config *config.ActorConfig, qm queueutil.QueueManager, qOpts *config.QueueOptions, handlerFunc HandlerFunc) *Manager {

	actorSystem := actor.NewActorSystem()

	manager := &Manager{
		config:      config,
		qm:          qm,
		actorSystem: actorSystem,
		qOpts:       qOpts,
		handlerFunc: handlerFunc,
		clusterName: clusterName,
	}

	svc := frame.FromContext(ctx)
	svc.AddCleanupMethod(manager.Shutdown)

	return manager
}

// Start initializes the actor system
func (m *Manager) Start(ctx context.Context) error {
	var err error

	svc := frame.FromContext(ctx)

	// Configure the actor system for remote capability
	remoteConfig := remote.Configure(m.config.Host, 0)

	// Configuration for the cluster provider
	var clusterProvider cluster.ClusterProvider

	if m.config.ClusterMode == "kubernetes" {
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

	managerProps := actor.PropsFromProducer(func() actor.Actor {
		return m
	})

	// Register the room actor kind with the cluster
	managerKind := cluster.NewKind(clusterManagerKind, managerProps)
	managerNodeId := clusterManagerID + "-" + frame.GenerateID(ctx)

	// Create the room actor props for the cluster
	roomProcessorKind := actorV1.NewRoomEventProcessorKind(func() actorV1.RoomEventProcessor {
		return NewRoomActor(ctx, m.cluster, svc, m.qm, m.handlerFunc)
	}, 0)

	// Create the cluster configuration
	clusterConfig := cluster.Configure(
		m.clusterName,
		clusterProvider,
		lookup,
		remoteConfig,
		cluster.WithKinds(managerKind, roomProcessorKind),
	)

	// Start the cluster
	m.cluster = cluster.New(m.actorSystem, clusterConfig)
	m.cluster.StartMember()

	//Ensure the room manager actor is active
	_ = m.cluster.Get(managerNodeId, clusterManagerKind)
	return nil
}

// EnsureRoomActorExists creates or gets a room actor for the specified room
func (m *Manager) EnsureRoomActorExists(ctx context.Context, roomID *spec.RoomID) error {

	m.cluster.Logger().With("room", roomID).Info("******************** Setup Room actor ***")

	roomQOpts, err := RoomifyQOpts(ctx, m.qOpts, roomID, true)
	if err != nil {
		return err
	}

	roomActor := actorV1.GetRoomEventProcessorGrainClient(m.cluster, roomID.String())
	_, err = roomActor.SetupRoom(&actorV1.SetupRoomRequest{
		RoomId:   roomID.String(),
		QueueRef: roomQOpts.QReference,
		QueuePfx: roomQOpts.Prefix,
		QueueUri: string(roomQOpts.DS),
	})
	if err != nil {
		return err
	}
	return nil
}

// handleRoomActorStarted is called when a room actor starts
func (m *Manager) handleRoomActorStarted(roomID string) {
	m.cluster.Logger().With("room", roomID).Info(" ***************************** Room actor started")
}

// handleRoomActorStopped is called when a room actor stops
func (m *Manager) handleRoomActorStopped(roomID string) {
	m.cluster.Logger().With("room", roomID).Info(" **************************** Room actor stopped")

}

// Receive handles messages sent to the actor system
func (m *Manager) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actorV1.ActorStarted:
		m.handleRoomActorStarted(msg.GetRoomId())
	case *actorV1.ActorStopped:
		m.handleRoomActorStopped(msg.GetRoomId())
	}
}

// Shutdown stops the actor system and all actors
func (m *Manager) Shutdown(ctx context.Context) {
	if m.cluster != nil {
		m.cluster.Shutdown(true)
	}
	if m.actorSystem != nil && !m.actorSystem.IsStopped() {
		m.actorSystem.Shutdown()
	}
}

// If a room consumer is inactive for a while then we will allow NATS
// to clean it up. This stops us from holding onto durable consumers
// indefinitely for rooms that might no longer be active, since they do
// have an interest overhead in the NATS Server. If the room becomes
// active again then we'll recreate the consumer anyway.
const inactiveThreshold = time.Hour * 24

// An event being processed by a room actor is only allowed 5 seconds maximum.
// If the event takes longer than this then we will assume it has issues and the message will be redelivered.
const maximumProcessingTime = time.Second * 15

// If a room actor is not receiving messages for sometime, we allow it to
// be stopped. This stops us from holding onto room actors indefinitely
// for rooms that might no longer be active, since they do have an
// interest overhead in the NATS Server. If the room becomes active
// again then we'll recreate the actor anyway.
const actorIdlingTime = time.Minute * 1

func RoomifyQOpts(_ context.Context, opts *config.QueueOptions, roomId *spec.RoomID, isSubscriber bool) (*config.QueueOptions, error) {

	ds := opts.DS
	if ds.IsNats() {

		subject := fmt.Sprintf("%s.%s", config.InputRoomEvent, queueutil.Tokenise(roomId.String()))
		ds = ds.ExtendQuery("subject", subject)

		if isSubscriber {
			ds = ds.ExtendQuery("consumer_filter_subject", subject)
			durable := fmt.Sprintf("RoomInput%s", queueutil.Tokenise(roomId.String()))

			ds = ds.ExtendQuery("consumer_durable_name", durable)
			ds = ds.ExtendQuery("consumer_deliver_policy", "all")
			ds = ds.ExtendQuery("consumer_ack_policy", "explicit")
			ds = ds.ExtendQuery("consumer_ack_wait", strconv.FormatInt(int64(maximumProcessingTime), 10))
			ds = ds.ExtendQuery("consumer_inactive_threshold", strconv.FormatInt(int64(inactiveThreshold), 10))
			ds = ds.ExtendQuery("consumer_headers_only", "false")
			ds = ds.ExtendQuery("receive_batch_max_batch_size", "1")
		} else {
			ds = ds.RemoveQuery("jetstream", "stream_storage", "stream_retention", "consumer_ack_policy",
				"consumer_deliver_policy", "consumer_headers_only", "consumer_replay_policy", "stream_name")
		}

	} else {
		ds = opts.DS.ExtendPath(queueutil.Tokenise(roomId.String()))
	}

	return &config.QueueOptions{
		QReference: fmt.Sprintf("%s%s", opts.QReference, queueutil.Tokenise(roomId.String())),
		Prefix:     opts.Prefix,
		DS:         ds,
	}, nil
}

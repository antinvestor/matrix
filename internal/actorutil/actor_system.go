package actorutil

import (
	"context"
	"log/slog"

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
	clusterName = "matrix-room-cluster"
)

// manager manages a set of actors for processing room events
type manager struct {
	config      *config.ActorOptions
	qm          queueutil.QueueManager
	actorSystem *actor.ActorSystem

	processors  map[ActorFunctionID]*functionOpt
	clusterName string
	cluster     *cluster.Cluster
}

// NewManager creates a new room actor system
func NewManager(ctx context.Context, config *config.ActorOptions, qm queueutil.QueueManager) (ActorManager, error) {

	svc := frame.Svc(ctx)

	actorSystem := actor.NewActorSystem(actor.WithLoggerFactory(
		func(sys *actor.ActorSystem) *slog.Logger {
			return svc.SLog(ctx)
		}))

	managerOptn := &manager{
		config:      config,
		qm:          qm,
		actorSystem: actorSystem,
		processors:  make(map[ActorFunctionID]*functionOpt),
		clusterName: clusterName,
	}

	svc.AddCleanupMethod(managerOptn.Shutdown)

	err := managerOptn.Start(ctx)
	if err != nil {
		return nil, err
	}

	return managerOptn, nil
}

func (m *manager) EnableFunction(functionID ActorFunctionID, qOpts *config.QueueOptions, handlerFunc HandlerFunc) {

	_, ok := m.processors[functionID]
	if ok {
		return
	}

	m.processors[functionID] = &functionOpt{
		qOpts:       qOpts,
		handlerFunc: handlerFunc,
	}
}

// Start initialises the actor system
func (m *manager) Start(ctx context.Context) error {
	var err error

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

	// Create the room actor props for the cluster
	roomProcessorKind := actorV1.NewRoomEventProcessorKind(func() actorV1.RoomEventProcessor {
		return NewRoomActor(ctx, m.cluster, m.qm, m.processors)
	}, 0)

	// Create the cluster configuration
	clusterConfig := cluster.Configure(
		m.clusterName,
		clusterProvider,
		lookup,
		remoteConfig,
		cluster.WithKinds(roomProcessorKind),
	)

	// Start the cluster
	m.cluster = cluster.New(m.actorSystem, clusterConfig)
	m.cluster.StartMember()

	return nil
}

// EnsureRoomActorExists creates or gets a room actor for the specified room
func (m *manager) EnsureRoomActorExists(ctx context.Context, functionID ActorFunctionID, roomID *spec.RoomID) error {
	_, err := m.createRoomActor(ctx, functionID, roomID)
	return err
}

func (m *manager) createRoomActor(_ context.Context, functionID ActorFunctionID, roomID *spec.RoomID) (*actorV1.RoomEventProcessorGrainClient, error) {

	actorID := prefixRoomIDWithFunc(functionID, roomID)
	roomActor := actorV1.GetRoomEventProcessorGrainClient(m.cluster, actorID)

	return roomActor, nil
}

// Progress obtains the subscription processing for a specific room
func (m *manager) Progress(ctx context.Context, functionID ActorFunctionID, roomID *spec.RoomID) (*actorV1.ProgressResponse, error) {
	roomActor, err := m.createRoomActor(ctx, functionID, roomID)
	if err != nil {
		return nil, err
	}

	resp, err := roomActor.Progress(&actorV1.ProgressRequest{
		RoomId: roomID.String(),
	})
	if err != nil {
		m.cluster.Logger().With("room", roomID).With("error", err).Error(" Failed to get to Room actor progress")
		return nil, err
	}
	return resp, nil
}

// Shutdown stops the actor system and all actors
func (m *manager) Shutdown(ctx context.Context) {
	if m.cluster != nil {
		m.cluster.Shutdown(true)
	}
	if m.actorSystem != nil && !m.actorSystem.IsStopped() {
		m.actorSystem.Shutdown()
	}
}

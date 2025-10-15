package actorutil

import (
	"context"
	"log/slog"

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
	wpm         queueutil.WorkPoolManager[any]
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
			log := svc.SLog(ctx)
			return log.With(slog.String("component", "actor"))
		}))

	managerOptn := &manager{
		config:      config,
		qm:          qm,
		wpm:         queueutil.NewWorkManager[any](svc),
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
	roomProcessorKind := actorV1.NewSequentialProcessorKind(func() actorV1.SequentialProcessor {
		return NewSeqActor(ctx, m.cluster, m.qm, m.wpm, m.processors)
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

func (m *manager) createSecActor(_ context.Context, actorID string) *actorV1.SequentialProcessorGrainClient {
	return actorV1.GetSequentialProcessorGrainClient(m.cluster, actorID)
}

// Progress obtains the subscription processing for a specific room
func (m *manager) Progress(ctx context.Context, functionID ActorFunctionID, id any) (*actorV1.ProgressResponse, error) {

	encodedIDStr, err := encodeID(id)
	if err != nil {
		return nil, err
	}

	clusterIDStr := encodeIDToClusterID(functionID, encodedIDStr)

	roomActor := m.createSecActor(ctx, clusterIDStr)

	resp, err := roomActor.Progress(&actorV1.ProgressRequest{
		Id: clusterIDStr,
	})
	if err != nil {
		m.cluster.Logger().
			With("cluster id", clusterIDStr).
			With("error", err).Error(" Failed to get to Room actor progress")
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

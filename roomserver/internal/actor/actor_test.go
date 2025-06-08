package actor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/roomserver/internal/actor"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type actorHeaders struct {
	t *testing.T
}

func (ah *actorHeaders) Handle(ctx context.Context, metadata map[string]string, message []byte) error {
	return nil
}

// TestRoomActorIntegration tests the integration between the actor system and room actors
func TestRoomActorIntegration(t *testing.T) {

	// Test cases for different scenarios
	tests := []struct {
		name          string
		roomID        string
		messageCount  int
		messagePrefix string
		sentMsgs      []map[string]any
		processedMsgs []map[string]any
	}{
		{
			name:          "process single message",
			roomID:        "!room1:test.com",
			messageCount:  1,
			messagePrefix: "single-message",
		},
		//{
		//	name:          "process multiple messages sequentially",
		//	roomID:        "!room2:test.com",
		//	messageCount:  3,
		//	messagePrefix: "sequential-message",
		//},
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {

				ctx, svc, cfg := testrig.Init(t, testOpts)
				defer svc.Stop(ctx)

				// Use the actual queue manager as requested
				qm := queueutil.NewQueueManager(svc)

				// Define the message handler function that will collect processed messages
				handlerFunc := func(ctx context.Context, metadata map[string]string, message []byte) error {

					t.Logf("  | +++++++++++++++++++++++++++ metadata [%v]", metadata)
					t.Logf("  | +++++++++++++++++++++++++++ message: %v", message)
					var msg map[string]any
					err := json.Unmarshal(message, &msg)
					if err != nil {
						return err
					}

					tc.processedMsgs = append(tc.processedMsgs, msg)
					return nil
				}

				roomCfg := &cfg.RoomServer
				// Create the actor system with the real queue manager
				// The field is ActorSystem, not Actor
				actorSystem := actor.NewManager(ctx, &roomCfg.ActorSystem, qm, &roomCfg.Queues.InputRoomEvent, handlerFunc)
				err := actorSystem.Start(ctx)
				require.NoError(t, err, "Failed to start actor system")

				// Get the queue URI for the room
				qopts := &cfg.RoomServer.Queues.InputRoomEvent

				err = qm.RegisterSubscriber(ctx, qopts, &actorHeaders{t: t})
				require.NoError(t, err, "Failed to register headers subscriber")

				t.Logf("  | +++++++++++++++++++++++++++ qopts: %v", qopts.DSrc())

				var roomID *spec.RoomID
				// Create a room ID
				roomID, err = spec.NewRoomID(tc.roomID)
				require.NoError(t, err, "Failed to create room ID")

				// Setup the actor by ensuring it exists
				err = actorSystem.EnsureRoomActorExists(ctx, roomID)
				require.NoError(t, err, "Failed to ensure room actor exists")

				roomOpts, err := actor.RoomifyQOpts(ctx, qopts, roomID, false)
				require.NoError(t, err, "Failed to roomify queue options for room ID to publish")

				t.Logf("  | +++++++++++++++++++++++++++ qopts: %v", qopts.DSrc())
				t.Logf("  | +++++++++++++++++++++++++++ roomOpts: %v", roomOpts.DSrc())

				err = qm.EnsurePublisherOk(ctx, roomOpts)
				require.NoError(t, err, "Failed to ensure publisher for room actor messages exists")

				// Generate and push messages to the queue
				for i := 0; i < tc.messageCount; i++ {
					messageID := fmt.Sprintf("%s-%d", tc.messagePrefix, i)

					message := map[string]any{
						"id":      messageID,
						"content": fmt.Sprintf("Test message content %d", i),
						"index":   float64(i),
					}

					metadata := map[string]string{
						"id":    messageID,
						"index": fmt.Sprintf("%d", i),
					}

					err = qm.Publish(ctx, roomOpts.Ref(), message, metadata)
					require.NoError(t, err, " | ++++++++++++++++++++++++++++++++++++++ Failed to publish message")

					tc.sentMsgs = append(tc.sentMsgs, message)
				}

				// Give a bit more time for any remaining processing
				for i := 0; i < 3; i++ {
					if len(tc.sentMsgs) == len(tc.processedMsgs) {
						break
					}
					time.Sleep(1 * time.Second)
				}

				assert.Equal(t, tc.messageCount, len(tc.sentMsgs), "Number of sent messages doesn't match expected")
				assert.Equal(t, tc.messageCount, len(tc.processedMsgs), "Number of processed messages doesn't match expected")

				// Verify message contents if needed
				for i := 0; i < len(tc.processedMsgs); i++ {

					assert.Equal(t, tc.sentMsgs[i], tc.processedMsgs[i], "Processed message doesn't match expected")
					assert.Equal(t, tc.processedMsgs[i]["index"], float64(i), "Processed message expected order")
					// The ID should match the expected format
					require.Contains(t, tc.processedMsgs[i], "id", "processed message doesn't have an ID")
					require.Contains(t, tc.processedMsgs[i], "content", "processed message doesn't have a content field")
					require.Contains(t, tc.processedMsgs[i], "index", "processed message doesn't have an Index")

				}
			})
		}
	})
}

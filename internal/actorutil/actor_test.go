package actorutil_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/antinvestor/gomatrixserverlib/spec"
	"github.com/antinvestor/matrix/internal/actorutil"
	"github.com/antinvestor/matrix/internal/queueutil"
	"github.com/antinvestor/matrix/setup/constants"
	"github.com/antinvestor/matrix/test"
	"github.com/antinvestor/matrix/test/testrig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		sentMsgs      []map[string]any
		processedMsgs []map[string]any
	}{
		{
			name:         "process single message",
			roomID:       "!room1:test.com",
			messageCount: 1,
		},
		{
			name:         "process multiple messages sequentially",
			roomID:       "!room2:test.com",
			messageCount: 3,
		},
	}

	test.WithAllDatabases(t, func(t *testing.T, testOpts test.DependancyOption) {

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {

				ctx, svc, cfg := testrig.Init(t, testOpts)
				defer svc.Stop(ctx)

				// Use the actual queue manager as requested
				qm := queueutil.NewQueueManager(svc)

				// Define the message handlerFunc function that will collect processed messages
				handlerFunc := func(ctx context.Context, metadata map[string]string, message []byte) error {

					var msg map[string]any
					err := json.Unmarshal(message, &msg)
					if err != nil {
						return err
					}

					tc.processedMsgs = append(tc.processedMsgs, msg)
					return nil
				}

				roomCfg := &cfg.RoomServer

				inputQOpts := &roomCfg.Queues.InputRoomEvent
				// Create the actor system with the real queue manager
				// The field is ActorSystem, not Actor
				am, err := actorutil.NewManager(ctx, &cfg.Global.Actors, qm)
				require.NoError(t, err, "Failed to create actor system manager")

				am.EnableFunction(actorutil.ActorFunctionRoomServer, &roomCfg.Queues.InputRoomEvent, handlerFunc)
				require.NoError(t, err, "Failed to enable actor system")

				err = qm.EnsurePublisherOk(ctx, inputQOpts)
				require.NoError(t, err, "Failed to ensure publisher is available")

				inputRoomEventsTopicRef := inputQOpts.Ref()

				var roomID *spec.RoomID
				// Create a room ID
				roomID, err = spec.NewRoomID(tc.roomID)
				require.NoError(t, err, "Failed to create room ID")

				_, err = am.Progress(ctx, actorutil.ActorFunctionRoomServer, roomID)
				require.NoError(t, err, "Failed to bootup room ID actor")

				// Generate and push messages to the queue
				for i := 0; i < tc.messageCount; i++ {

					message := map[string]any{
						"id":      fmt.Sprintf("id-%d", i),
						"content": fmt.Sprintf("Test message content %d", i),
						"index":   float64(i),
					}

					metadata := map[string]string{
						constants.RoomID: constants.EncodeRoomID(roomID),
						"index":          fmt.Sprintf("%d", i),
					}

					err = qm.Publish(ctx, inputRoomEventsTopicRef, message, metadata)
					require.NoError(t, err, "Failed to publish message")

					tc.sentMsgs = append(tc.sentMsgs, message)
				}

				// Give a bit more time for any remaining processing
				for i := 0; i < 10; i++ {
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

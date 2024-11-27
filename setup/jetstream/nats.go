package jetstream

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/antinvestor/matrix/setup/config"
	"github.com/antinvestor/matrix/setup/process"
	"github.com/getsentry/sentry-go"
	"github.com/sirupsen/logrus"

	natsclient "github.com/nats-io/nats.go"
)

type NATSInstance struct {
	nc *natsclient.Conn
	js natsclient.JetStreamContext
}

func DeleteAllStreams(js natsclient.JetStreamContext, cfg *config.JetStream) {
	for _, stream := range streams { // streams are defined in streams.go
		name := cfg.Prefixed(stream.Name)
		_ = js.DeleteStream(name)
	}
}

func (s *NATSInstance) Prepare(process *process.ProcessContext, cfg *config.JetStream) (natsclient.JetStreamContext, *natsclient.Conn) {
	// check if we need an in-process NATS Server
	if len(cfg.Addresses) == 0 {
		logrus.Fatalf("No Jetstream addresses specified")
	}
	// reuse existing connections
	if s.nc != nil {
		return s.js, s.nc
	}
	var err error
	s.js, s.nc, err = setupNATS(process, cfg, nil)
	if err != nil {
		logrus.WithError(err).Fatalf("Could not setup nats at : %s", cfg.Addresses)
	}
	return s.js, s.nc

}

// nolint:gocyclo
func setupNATS(_ *process.ProcessContext, cfg *config.JetStream, nc *natsclient.Conn) (natsclient.JetStreamContext, *natsclient.Conn, error) {
	if nc == nil {
		var err error
		var opts []natsclient.Option

		if string(cfg.Credentials) != "" {
			opts = append(opts, natsclient.UserCredentials(string(cfg.Credentials)))
		}
		nc, err = natsclient.Connect(strings.Join(cfg.Addresses, ","), opts...)
		if err != nil {
			return nil, nil, err
		}
	}

	s, err := nc.JetStream()
	if err != nil {
		return nil, nil, err
	}

	var info *natsclient.StreamInfo
	for _, stream := range streams { // streams are defined in streams.go
		name := cfg.Prefixed(stream.Name)
		info, err = s.StreamInfo(name)
		if err != nil && !errors.Is(err, natsclient.ErrStreamNotFound) {
			logrus.WithError(err).Fatal("Unable to get stream info")
		}
		subjects := stream.Subjects
		if len(subjects) == 0 {
			// By default we want each stream to listen for the subjects
			// that are either an exact match for the stream name, or where
			// the first part of the subject is the stream name. ">" is a
			// wildcard in NATS for one or more subject tokens. In the case
			// that the stream is called "Foo", this will match any message
			// with the subject "Foo", "Foo.Bar" or "Foo.Bar.Baz" etc.
			subjects = []string{name, name + ".>"}
		}
		if info != nil {
			// If the stream config doesn't match what we expect, try to update
			// it. If that doesn't work then try to blow it away and we'll then
			// recreate it in the next section.
			// Each specific option that we set must be checked by hand, as if
			// you DeepEqual the whole config struct, it will always show that
			// there's a difference because the NATS Server will return defaults
			// in the stream info.
			switch {
			case !reflect.DeepEqual(info.Config.Subjects, subjects):
				fallthrough
			case info.Config.Retention != stream.Retention:
				fallthrough
			case info.Config.Storage != stream.Storage:
				fallthrough
			case info.Config.MaxAge != stream.MaxAge:
				// Try updating the stream first, as many things can be updated
				// non-destructively.
				if info, err = s.UpdateStream(stream); err != nil {
					logrus.WithError(err).Warnf("Unable to update stream %q, recreating...", name)
					// We failed to update the stream, this is a last attempt to get
					// things working but may result in data loss.
					if err = s.DeleteStream(name); err != nil {
						logrus.WithError(err).Fatalf("Unable to delete stream %q", name)
					}
					info = nil
				}
			}
		}
		if info == nil {

			// Namespace the streams without modifying the original streams
			// array, otherwise we end up with namespaces on namespaces.
			namespaced := *stream
			namespaced.Name = name
			namespaced.Subjects = subjects
			if _, err = s.AddStream(&namespaced); err != nil {
				logger := logrus.WithError(err).WithFields(logrus.Fields{
					"stream":   namespaced.Name,
					"subjects": namespaced.Subjects,
				})

				// The stream was supposed to be on disk. Let's try starting
				// Dendrite with the stream in-memory instead. That'll mean that
				// we can't recover anything that was queued on the disk but we
				// will still be able to start and run hopefully in the meantime.
				sentry.CaptureException(fmt.Errorf("unable to add stream %q: %w", namespaced.Name, err))
				logger.WithError(err).Fatal("Unable to add stream")
			}
		}
	}

	// Clean up old consumers so that interest-based consumers do the
	// right thing.
	for stream, consumers := range map[string][]string{
		OutputClientData:        {"SyncAPIClientAPIConsumer"},
		OutputReceiptEvent:      {"SyncAPIEDUServerReceiptConsumer", "FederationAPIEDUServerConsumer"},
		OutputSendToDeviceEvent: {"SyncAPIEDUServerSendToDeviceConsumer", "FederationAPIEDUServerConsumer"},
		OutputTypingEvent:       {"SyncAPIEDUServerTypingConsumer", "FederationAPIEDUServerConsumer"},
		OutputRoomEvent:         {"AppserviceRoomserverConsumer"},
		OutputStreamEvent:       {"UserAPISyncAPIStreamEventConsumer"},
		OutputReadUpdate:        {"UserAPISyncAPIReadUpdateConsumer"},
	} {
		streamName := cfg.Matrix.JetStream.Prefixed(stream)
		for _, consumer := range consumers {
			consumerName := cfg.Matrix.JetStream.Prefixed(consumer) + "Pull"
			consumerInfo, err := s.ConsumerInfo(streamName, consumerName)
			if err != nil || consumerInfo == nil {
				continue
			}
			if err = s.DeleteConsumer(streamName, consumerName); err != nil {
				logrus.WithError(err).Errorf("Unable to clean up old consumer %q for stream %q", consumer, stream)
			}
		}
	}

	return s, nc, nil
}

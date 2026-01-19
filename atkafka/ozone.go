package atkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/ozone"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/twmb/franz-go/pkg/kgo"
)

type OzoneServer struct {
	logger           *slog.Logger
	bootstrapServers []string
	outputTopic      string
	ozoneClientArgs  *OzoneClientArgs

	cursorMu sync.Mutex
	cursor   string
}

type OzoneServerArgs struct {
	BootstrapServers []string
	OutputTopic      string
	Logger           *slog.Logger
	OzoneClientArgs  *OzoneClientArgs
}

func NewOzoneServer(args *OzoneServerArgs) (*OzoneServer, error) {
	s := OzoneServer{
		logger:           args.Logger,
		bootstrapServers: args.BootstrapServers,
		outputTopic:      args.OutputTopic,
		ozoneClientArgs:  args.OzoneClientArgs,
	}

	return &s, nil
}

func (s *OzoneServer) Run(ctx context.Context) error {
	logger := s.logger.With("name", "Run")

	if err := s.LoadCursor(); err != nil {
		logger.Error("error loading cursor", "err", err)
	}

	ozoneClient, err := NewOzoneClient(s.ozoneClientArgs)
	if err != nil {
		return err
	}

	logger.Info("creating producer", "bootstrap-servers", s.bootstrapServers, "output-topic", s.outputTopic)
	busProducer, err := NewProducer(
		ctx,
		s.logger.With("component", "producer"),
		s.bootstrapServers,
		s.outputTopic,
		WithEnsureTopic(true),
		WithTopicPartitions(4),
	)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer busProducer.close()

	shutdownPoll := make(chan struct{}, 1)
	pollShutdown := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(1 * time.Second)

		defer func() {
			ticker.Stop()
			close(pollShutdown)
		}()

		for {
			select {
			case <-ctx.Done():
				logger.Info("shutting down poller on context cancel")
				return
			case <-shutdownPoll:
				logger.Info("shutting down poller")
				return
			case <-ticker.C:
				func() {
					ozoneClient.mu.Lock()
					defer ozoneClient.mu.Unlock()

					cursor := s.GetCursor()

					logger.Info("attempting to fetch next batch of events")

					_, events, err := ozoneClient.GetEvents(ctx, cursor)
					if err != nil {
						logger.Error("error getting events", "err", err)
						return
					}

					if len(events) == 0 {
						logger.Info("no new events to emit")
						return
					}

					var newCursor string
					for _, evt := range events {
						func() {
							status := "error"

							defer func() {
								handledEvents.WithLabelValues("ozone", status).Inc()
							}()

							b, err := json.Marshal(evt)
							if err != nil {
								logger.Error("error marshaling event", "err", err)
								return
							}

							if err := busProducer.ProduceAsync(ctx, strconv.FormatInt(evt.Id, 10), b, func(r *kgo.Record, err error) {
								produceStatus := "error"

								defer func() {
									producedEvents.WithLabelValues(produceStatus).Inc()
								}()

								if err != nil {
									logger.Error("error producing event", "err", err)
									return
								}
								produceStatus = "ok"
							}); err != nil {
								logger.Error("error attempting to produce event", "err", err)
							}

							newCursor = evt.CreatedAt

							status = "ok"
						}()
					}

					if err := s.UpdateCursor(newCursor); err != nil {
						logger.Error("error updating cursor", "err", err)
					}

					logger.Info("received events to emit", "length", len(events), "new-cursor", newCursor)
				}()
			}
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-ctx.Done():
		logger.Warn("context cancelled")
		close(shutdownPoll)
	case sig := <-signals:
		logger.Info("received exit signal", "signal", sig)
		close(shutdownPoll)
	}

	select {
	case <-pollShutdown:
		logger.Info("shutdown successfully")
	case <-time.After(5 * time.Second):
		logger.Error("poller did not shut down within five seconds, forcing shutdown")
	}

	return nil
}

func (s *OzoneServer) UpdateCursor(cursor string) error {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()

	// HACK: do this so we avoid fetching the same last item with after. ideally would use the id or something
	// from the response, but i am lazy for now in this and its extremely unlikely to happen anyway
	timestamp, err := time.Parse(time.RFC3339Nano, cursor)
	if err != nil {
		return fmt.Errorf("failed to parse timestmap: %w", err)
	}
	cursor = timestamp.Add(time.Millisecond).UTC().Format(time.RFC3339Nano)

	s.cursor = cursor
	if err := os.WriteFile("ozone-cursor.txt", []byte(cursor), 0644); err != nil {
		return fmt.Errorf("failed to write cursor: %w", err)
	}
	return nil
}

func (s *OzoneServer) LoadCursor() error {
	logger := s.logger.With("name", "LoadCursor")

	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()

	logger.Info("attempting to load stored cursor")

	b, err := os.ReadFile("ozone-cursor.txt")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.Info("no previous cursor found")
			return nil
		}
		return fmt.Errorf("failed to load cursor: %w", err)
	}
	s.cursor = strings.TrimSpace(string(b))
	logger.Info("loaded old cursor", "cursor", s.cursor)
	return nil
}

func (s *OzoneServer) GetCursor() string {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	return s.cursor
}

type OzoneClient struct {
	xrpcc  *xrpc.Client
	mu     sync.Mutex
	logger *slog.Logger
}

type OzoneClientArgs struct {
	OzonePdsHost    string
	OzoneIdentifier string
	OzonePassword   string
	OzoneLabelerDid string
	Logger          *slog.Logger
}

func NewOzoneClient(args *OzoneClientArgs) (*OzoneClient, error) {
	xrpcc := &xrpc.Client{
		Host: args.OzonePdsHost,
		Headers: map[string]string{
			"atproto-proxy": fmt.Sprintf("%s#atproto_labeler", args.OzoneLabelerDid),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := atproto.ServerCreateSession(ctx, xrpcc, &atproto.ServerCreateSession_Input{
		Identifier: args.OzoneIdentifier,
		Password:   args.OzonePassword,
	})
	if err != nil {
		return nil, err
	}

	xrpcc.Auth = &xrpc.AuthInfo{
		Handle:     resp.Handle,
		Did:        resp.Did,
		AccessJwt:  resp.AccessJwt,
		RefreshJwt: resp.RefreshJwt,
	}

	oc := OzoneClient{
		xrpcc:  xrpcc,
		logger: args.Logger,
	}

	go func() {
		logger := args.Logger.With("component", "refresh-routine")

		ticker := time.NewTicker(1 * time.Hour)

		defer func() {
			ticker.Stop()
		}()

		for range ticker.C {
			func() {
				logger.Info("attempting to refresh session")

				oc.mu.Lock()
				defer oc.mu.Unlock()

				xrpcc.Auth.AccessJwt = xrpcc.Auth.RefreshJwt

				refreshCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				resp, err := atproto.ServerRefreshSession(refreshCtx, xrpcc)
				if err != nil {
					logger.Error("error refreshing session", "err", err)
					return
				}

				xrpcc.Auth.AccessJwt = resp.AccessJwt
				xrpcc.Auth.RefreshJwt = resp.RefreshJwt

				logger.Info("session refreshed successfully")
			}()
		}
	}()

	return &oc, nil
}

func (oc *OzoneClient) GetEvents(ctx context.Context, before string) (*string, []*ozone.ModerationDefs_ModEventView, error) {
	resp, err := ozone.ModerationQueryEvents(
		ctx,
		oc.xrpcc,
		nil,    // labels array
		nil,    // tags array
		"",     // age assurance state
		"",     // batch id
		nil,    // collections array
		"",     // comment filter
		before, // created before
		"",     // created after
		"",     // created by
		"",     // cursor
		false,  // has comment
		true,   // include all records
		100,    // limit
		nil,    // mod tool array
		nil,    // policies array
		nil,    // removed labels array
		nil,    // removed tags array
		nil,    // report types array
		"asc",  // sort direction
		"",     // subject
		"",     // subject type
		nil,    // types array
		false,  // withStrike bool
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error querying ozone events: %w", err)
	}

	return resp.Cursor, resp.Events, nil
}

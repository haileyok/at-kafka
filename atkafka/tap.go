package atkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/araddon/dateparse"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/gorilla/websocket"
	"golang.org/x/sync/semaphore"
)

func (s *Server) RunTapMode(ctx context.Context) error {
	sema := semaphore.NewWeighted(1_000)

	s.logger.Info("starting tap consumer", "tap-host", s.tapHost, "bootstrap-servers", s.bootstrapServers, "output-topic", s.outputTopic)

	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	producerLogger := s.logger.With("component", "producer")
	kafProducer, err := NewProducer(createCtx, producerLogger, s.bootstrapServers, s.outputTopic,
		WithEnsureTopic(true),
		WithTopicPartitions(200),
	)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer kafProducer.Close()
	s.producer = kafProducer
	s.logger.Info("created producer")

	wsDialer := websocket.DefaultDialer
	u, err := url.Parse(s.tapHost)
	if err != nil {
		return fmt.Errorf("invalid tapHost: %w", err)
	}
	u.Path = "/channel"
	s.logger.Info("created dialer")

	wsErr := make(chan error, 1)
	shutdownWs := make(chan struct{}, 1)
	go func() {
		logger := s.logger.With("component", "websocket")

		logger.Info("subscribing to tap stream", "upstream", s.tapHost)

		conn, _, err := wsDialer.Dial(u.String(), http.Header{
			"User-Agent": []string{"at-kafka/0.0.0"},
		})
		if err != nil {
			wsErr <- err
			return
		}

		// handle events!
		for {
			var evt TapEvent
			err := conn.ReadJSON(&evt)
			if err != nil {
				logger.Error("error reading json from websocket", "err", err)
				break
			}

			if err := sema.Acquire(ctx, 1); err != nil {
				logger.Error("error acquring sema", "err", err)
				break
			}

			go func() {
				defer sema.Release(1)
				s.handleTapEvent(ctx, &evt)
			}()
		}

		<-shutdownWs

		wsErr <- nil
	}()
	s.logger.Info("created tap consumer")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-signals:
		s.logger.Info("shutting down on signal", "signal", sig)
	case err := <-wsErr:
		if err != nil {
			s.logger.Error("websocket error", "err", err)
		} else {
			s.logger.Info("websocket shutdown unexpectedly")
		}
	}

	close(shutdownWs)

	return nil
}

func (s *Server) handleTapEvent(ctx context.Context, evt *TapEvent) error {
	logger := s.logger.With("component", "handleEvent")

	var collection string
	var actionName string

	var evtKey string
	var evtsToProduce [][]byte

	if evt.Record != nil {
		// key events by DID
		evtKey = evt.Record.Did
		did := evt.Record.Did
		kind := evt.Record.Action
		collection = evt.Record.Collection
		rkey := evt.Record.Rkey
		atUri := fmt.Sprintf("at://%s/%s/%s", did, collection, rkey)

		skip := false
		if len(s.watchedCollections) > 0 {
			skip = true
			for _, watchedCollection := range s.watchedCollections {
				if watchedCollection == collection || strings.HasPrefix(collection, watchedCollection+".") {
					skip = false
					break
				}
			}
		} else if len(s.ignoredCollections) > 0 {
			for _, ignoredCollection := range s.ignoredCollections {
				if ignoredCollection == collection || strings.HasPrefix(collection, ignoredCollection+".") {
					skip = true
					break
				}
			}
		}

		if skip {
			logger.Debug("skipping event based on collection", "collection", collection)
			return nil
		}

		actionName = "operation#" + kind

		handledEvents.WithLabelValues(actionName, collection).Inc()

		// create the formatted operation
		atkOp := AtKafkaOp{
			Action:     evt.Record.Action,
			Collection: collection,
			Rkey:       rkey,
			Uri:        atUri,
			Cid:        evt.Record.Cid,
			Path:       fmt.Sprintf("%s/%s", collection, rkey),
		}

		if evt.Record.Record != nil {
			atkOp.Record = *evt.Record.Record
		}

		kafkaEvt := AtKafkaEvent{
			Did:       did,
			Operation: &atkOp,
		}

		if evt.Record.Record != nil {
			timestamp, err := parseTimeFromRecord(collection, *evt.Record.Record, rkey)
			if err != nil {
				return fmt.Errorf("error getting timestamp from record: %w", err)
			}
			kafkaEvt.Timestamp = timestamp.Format(time.RFC3339Nano)
		}

		evtBytes, err := json.Marshal(&kafkaEvt)
		if err != nil {
			return fmt.Errorf("failed to marshal kafka event: %w", err)
		}

		evtsToProduce = append(evtsToProduce, evtBytes)
	}

	for _, evtBytes := range evtsToProduce {
		if err := s.produceAsync(ctx, evtKey, evtBytes); err != nil {
			return err
		}
	}

	return nil
}

func parseTimeFromRecord(collection string, rec map[string]any, rkey string) (*time.Time, error) {
	var rkeyTime time.Time
	if rkey != "self" {
		rt, err := syntax.ParseTID(rkey)
		if err == nil {
			rkeyTime = rt.Time()
		}
	}

	switch collection {
	case "app.bsky.feed.post":
		cat, ok := rec["createdAt"].(string)
		if ok {
			t, err := dateparse.ParseAny(cat)
			if err == nil {
				return &t, nil
			}

			if rkeyTime.IsZero() {
				return timePtr(time.Now()), nil
			}
		}

		return &rkeyTime, nil
	case "app.bsky.feed.repost":
		cat, ok := rec["createdAt"].(string)
		if ok {
			t, err := dateparse.ParseAny(cat)
			if err == nil {
				return &t, nil
			}

			if rkeyTime.IsZero() {
				return nil, fmt.Errorf("failed to get a useful timestamp from record")
			}
		}

		return &rkeyTime, nil
	case "app.bsky.feed.like":
		cat, ok := rec["createdAt"].(string)
		if ok {
			t, err := dateparse.ParseAny(cat)
			if err == nil {
				return &t, nil
			}

			if rkeyTime.IsZero() {
				return nil, fmt.Errorf("failed to get a useful timestamp from record")
			}
		}

		return &rkeyTime, nil
	case "app.bsky.actor.profile":
		// We can't really trust the createdat in the profile record anyway, and its very possible its missing. just use iat for this one
		return timePtr(time.Now()), nil
	case "app.bsky.feed.generator":
		if !rkeyTime.IsZero() {
			return &rkeyTime, nil
		}
		return timePtr(time.Now()), nil
	default:
		if !rkeyTime.IsZero() {
			return &rkeyTime, nil
		}
		return timePtr(time.Now()), nil
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}

package atkafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/gorilla/websocket"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Server struct {
	relayHost        string
	bootstrapServers []string
	outputTopic      string
	ospreyCompat     bool
	logger           *slog.Logger

	producer *Producer

	plcClient *PlcClient
}

type ServerArgs struct {
	RelayHost        string
	PlcHost          string
	BootstrapServers []string
	OutputTopic      string
	OspreyCompat     bool
	Logger           *slog.Logger
}

func NewServer(args *ServerArgs) *Server {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	var plcClient *PlcClient
	if args.PlcHost != "" {
		plcClient = NewPlcClient(&PlcClientArgs{
			PlcHost: args.PlcHost,
		})
	}

	return &Server{
		relayHost:        args.RelayHost,
		plcClient:        plcClient,
		bootstrapServers: args.BootstrapServers,
		outputTopic:      args.OutputTopic,
		ospreyCompat:     args.OspreyCompat,
		logger:           args.Logger,
	}
}

func (s *Server) Run(ctx context.Context) error {
	s.logger.Info("starting consumer", "relay-host", s.relayHost, "bootstrap-servers", s.bootstrapServers, "output-topic", s.outputTopic)

	createCtx, _ := context.WithTimeout(ctx, time.Second*5)

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
	u, err := url.Parse(s.relayHost)
	if err != nil {
		return fmt.Errorf("invalid relayHost: %w", err)
	}
	u.Path = "/xrpc/com.atproto.sync.subscribeRepos"
	s.logger.Info("created dialer")

	wsErr := make(chan error, 1)
	shutdownWs := make(chan struct{}, 1)
	go func() {
		logger := s.logger.With("component", "websocket")

		logger.Info("subscribing to repo event stream", "upstream", s.relayHost)

		conn, _, err := wsDialer.Dial(u.String(), http.Header{
			"User-Agent": []string{"at-kafka/0.0.0"},
		})
		if err != nil {
			wsErr <- err
			return
		}

		parallelism := 400
		scheduler := parallel.NewScheduler(parallelism, 1000, s.relayHost, s.handleEvent)
		defer scheduler.Shutdown()

		logger.Info("firehose scheduler configured", "parallelism", parallelism)

		go func() {
			if err := events.HandleRepoStream(ctx, conn, scheduler, logger); err != nil {
				wsErr <- err
				return
			}
		}()

		<-shutdownWs

		wsErr <- nil
	}()
	s.logger.Info("created relay consumer")

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

type EventMetadata struct {
	DidDocument  *identity.DIDDocument `json:"didDocument,omitempty"`
	PdsHost      string                `json:"pdsHost,omitempty"`
	Handle       string                `json:"handle,omitempty"`
	DidCreatedAt string                `json:"didCreatedAt,omitempty"`
	AccountAge   int64                 `json:"accountAge"`
}

func (s *Server) FetchEventMetadata(ctx context.Context, did string) (*EventMetadata, error) {
	var didDocument *identity.DIDDocument
	var pdsHost string
	var handle string
	var didCreatedAt string
	accountAge := int64(-1)

	var wg sync.WaitGroup

	if s.plcClient != nil {
		wg.Go(func() {
			logger := s.logger.With("component", "didDoc")
			doc, err := s.plcClient.GetDIDDoc(ctx, did)
			if err != nil {
				logger.Error("error fetching did doc", "did", did, "err", err)
				return
			}
			didDocument = doc

			for _, svc := range doc.Service {
				if svc.ID == "#atproto_pds" {
					pdsHost = svc.ServiceEndpoint
					break
				}
			}

			for _, aka := range doc.AlsoKnownAs {
				if strings.HasPrefix(aka, "at://") {
					handle = strings.TrimPrefix(aka, "at://")
					break
				}
			}
		})

		wg.Go(func() {
			logger := s.logger.With("component", "auditLog")
			auditLog, err := s.plcClient.GetDIDAuditLog(ctx, did)
			if err != nil {
				logger.Error("error fetching did audit log", "did", did, "err", err)
				return
			}

			didCreatedAt = auditLog.CreatedAt

			createdAt, err := time.Parse(time.RFC3339Nano, auditLog.CreatedAt)
			if err != nil {
				logger.Error("error parsing timestamp in audit log", "did", did, "timestamp", auditLog.CreatedAt, "err", err)
				return
			}

			accountAge = int64(time.Since(createdAt).Seconds())
		})
	}

	wg.Wait()

	return &EventMetadata{
		DidDocument:  didDocument,
		PdsHost:      pdsHost,
		Handle:       handle,
		DidCreatedAt: didCreatedAt,
		AccountAge:   accountAge,
	}, nil
}

func (s *Server) handleEvent(ctx context.Context, evt *events.XRPCStreamEvent) error {
	dispatchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := s.logger.With("component", "handleEvent")
	logger.Debug("event", "seq", evt.Sequence())

	var collection string
	var actionName string

	if evt.RepoCommit != nil {
		// read the repo
		rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.RepoCommit.Blocks))
		if err != nil {
			logger.Error("failed to read repo from car", "error", err)
			return nil
		}

		for _, op := range evt.RepoCommit.Ops {
			kind := repomgr.EventKind(op.Action)
			collection = strings.Split(op.Path, "/")[0]
			rkey := strings.Split(op.Path, "/")[1]
			atUri := fmt.Sprintf("at://%s/%s/%s", evt.RepoCommit.Repo, collection, rkey)

			kindStr := "create"
			switch kind {
			case repomgr.EvtKindUpdateRecord:
				kindStr = "update"
			case repomgr.EvtKindDeleteRecord:
				kindStr = "delete"
			}
			actionName = "operation#" + kindStr

			handledEvents.WithLabelValues(actionName, collection).Inc()

			var rec map[string]any
			var recCid string
			if kind == repomgr.EvtKindCreateRecord || kind == repomgr.EvtKindUpdateRecord {
				rcid, recB, err := rr.GetRecordBytes(ctx, op.Path)
				if err != nil {
					logger.Error("failed to get record bytes", "error", err)
					continue
				}

				// verify the cids match
				recCid = rcid.String()
				if recCid != op.Cid.String() {
					logger.Error("record cid mismatch", "expected", *op.Cid, "actual", rcid)
					continue
				}

				// unmarshal the cbor into a map[string]any
				maybeRec, err := atdata.UnmarshalCBOR(*recB)
				if err != nil {
					logger.Error("failed to unmarshal record", "error", err)
					continue
				}
				rec = maybeRec
			}

			// create the formatted operation
			atkOp := AtKafkaOp{
				Action:     op.Action,
				Collection: collection,
				Rkey:       rkey,
				Uri:        atUri,
				Cid:        recCid,
				Path:       op.Path,
				Record:     rec,
			}

			// create the evt to put on kafka, regardless of if we are using osprey or not
			kafkaEvt := AtKafkaEvent{
				Did:       evt.RepoCommit.Repo,
				Timestamp: evt.RepoCommit.Time,
				Operation: &atkOp,
			}

			eventMetadata, err := s.FetchEventMetadata(dispatchCtx, evt.RepoCommit.Repo)
			if err != nil {
				logger.Error("error fetching event metadata", "err", err)
			} else {
				kafkaEvt.Metadata = eventMetadata
			}

			var kafkaEvtBytes []byte
			if s.ospreyCompat {
				// create the wrapper event for osprey
				ospreyKafkaEvent := OspreyAtKafkaEvent{
					Data: OspreyEventData{
						ActionName: actionName,
						ActionId:   time.Now().UnixNano(), // TODO: this should be a snowflake
						Data:       kafkaEvt,
						Timestamp:  evt.RepoCommit.Time,
						SecretData: map[string]string{},
						Encoding:   "UTF8",
					},
					SendTime: time.Now().Format(time.RFC3339),
				}

				kafkaEvtBytes, err = json.Marshal(&ospreyKafkaEvent)
			} else {
				kafkaEvtBytes, err = json.Marshal(&kafkaEvt)
			}
			if err != nil {
				return fmt.Errorf("failed to marshal kafka event: %w", err)
			}

			if err := s.produceAsync(ctx, evt.RepoCommit.Repo, kafkaEvtBytes); err != nil {
				return err
			}
		}
	} else {
		defer func() {
			handledEvents.WithLabelValues(actionName, "").Inc()
		}()

		// start with a kafka event and an action name
		var kafkaEvt AtKafkaEvent
		var timestamp string
		var did string

		if evt.RepoAccount != nil {
			actionName = "account"
			timestamp = evt.RepoAccount.Time
			did = evt.RepoAccount.Did

			kafkaEvt = AtKafkaEvent{
				Did:       evt.RepoAccount.Did,
				Timestamp: evt.RepoAccount.Time,
				Account: &AtKafkaAccount{
					Active: evt.RepoAccount.Active,
					Seq:    evt.RepoAccount.Seq,
					Status: evt.RepoAccount.Status,
				},
			}
		} else if evt.RepoIdentity != nil {
			actionName = "identity"
			timestamp = evt.RepoIdentity.Time
			did = evt.RepoIdentity.Did

			kafkaEvt = AtKafkaEvent{
				Did:       evt.RepoIdentity.Did,
				Timestamp: evt.RepoIdentity.Time,
				Identity: &AtKafkaIdentity{
					Seq:    evt.RepoIdentity.Seq,
					Handle: *evt.RepoIdentity.Handle,
				},
			}
		} else if evt.RepoInfo != nil {
			actionName = "info"
			timestamp = time.Now().Format(time.RFC3339Nano)

			kafkaEvt = AtKafkaEvent{
				Info: &AtKafkaInfo{
					Name:    evt.RepoInfo.Name,
					Message: evt.RepoInfo.Message,
				},
			}
		} else {
			return fmt.Errorf("unhandled event received")
		}

		if did != "" {
			eventMetadata, err := s.FetchEventMetadata(dispatchCtx, did)
			if err != nil {
				logger.Error("error fetching event metadata", "err", err)
			} else {
				kafkaEvt.Metadata = eventMetadata
			}
		}

		// create the kafka event bytes
		var kafkaEvtBytes []byte
		var err error

		if s.ospreyCompat {
			// wrap the event in an osprey event
			ospreyKafkaEvent := OspreyAtKafkaEvent{
				Data: OspreyEventData{
					ActionName: actionName,
					ActionId:   time.Now().UnixNano(), // TODO: this should be a snowflake
					Data:       kafkaEvt,
					Timestamp:  timestamp,
					SecretData: map[string]string{},
					Encoding:   "UTF8",
				},
				SendTime: time.Now().Format(time.RFC3339),
			}

			kafkaEvtBytes, err = json.Marshal(&ospreyKafkaEvent)
		} else {
			kafkaEvtBytes, err = json.Marshal(&kafkaEvt)
		}
		if err != nil {
			return fmt.Errorf("failed to marshal kafka event: %w", err)
		}

		if err := s.produceAsync(ctx, did, kafkaEvtBytes); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) produceAsync(ctx context.Context, key string, msg []byte) error {
	callback := func(r *kgo.Record, err error) {
		status := "ok"
		if err != nil {
			status = "error"
			s.logger.Error("error producing message", "err", err)
		}
		producedEvents.WithLabelValues(status).Inc()
	}

	if !s.ospreyCompat {
		if err := s.producer.ProduceAsync(ctx, key, msg, callback); err != nil {
			return fmt.Errorf("failed to produce message: %w", err)
		}
	} else if s.ospreyCompat {
		if err := s.producer.ProduceAsync(ctx, key, msg, callback); err != nil {
			return fmt.Errorf("failed to produce message: %w", err)
		}
	}

	return nil
}

type AtKafkaOp struct {
	Action     string         `json:"action"`
	Collection string         `json:"collection"`
	Rkey       string         `json:"rkey"`
	Uri        string         `json:"uri"`
	Cid        string         `json:"cid"`
	Path       string         `json:"path"`
	Record     map[string]any `json:"record"`
}

type AtKafkaIdentity struct {
	Seq    int64  `json:"seq"`
	Handle string `json:"handle"`
}

type AtKafkaInfo struct {
	Name    string  `json:"name"`
	Message *string `json:"message,omitempty"`
}

type AtKafkaAccount struct {
	Active bool    `json:"active"`
	Seq    int64   `json:"seq"`
	Status *string `json:"status,omitempty"`
}

type AtKafkaEvent struct {
	Did       string         `json:"did"`
	Timestamp string         `json:"timestamp"`
	Metadata  *EventMetadata `json:"eventMetadata"`

	Operation *AtKafkaOp       `json:"operation,omitempty"`
	Account   *AtKafkaAccount  `json:"account,omitempty"`
	Identity  *AtKafkaIdentity `json:"identity,omitempty"`
	Info      *AtKafkaInfo     `json:"info,omitempty"`
}

// Intentionally using snake case since that is what Osprey expects
type OspreyEventData struct {
	ActionName string            `json:"action_name"`
	ActionId   int64             `json:"action_id"`
	Data       AtKafkaEvent      `json:"data"`
	Timestamp  string            `json:"timestamp"`
	SecretData map[string]string `json:"secret_data"`
	Encoding   string            `json:"encoding"`
}

type OspreyAtKafkaEvent struct {
	Data     OspreyEventData `json:"data"`
	SendTime string          `json:"send_time"`
}

package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bluesky-social/go-util/pkg/telemetry"
	"github.com/haileyok/at-kafka/atkafka"
	_ "github.com/joho/godotenv/autoload"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name: "at-kafka",
		Flags: []cli.Flag{
			telemetry.CLIFlagDebug,
			telemetry.CLIFlagMetricsListenAddress,
			&cli.StringFlag{
				Name:    "relay-host",
				Usage:   "Websocket host to subscribe to for events",
				Value:   "wss://bsky.network",
				EnvVars: []string{"ATKAFKA_RELAY_HOST"},
			},
			&cli.StringSliceFlag{
				Name:     "bootstrap-servers",
				Usage:    "List of Kafka bootstrap servers",
				EnvVars:  []string{"ATKAFKA_BOOTSTRAP_SERVERS"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "output-topic",
				Usage:    "The Kafka topic to produce events to",
				EnvVars:  []string{"ATKAFKA_OUTPUT_TOPIC"},
				Required: true,
			},
			&cli.BoolFlag{
				Name:    "osprey-compatible",
				Usage:   "Whether or not events should be formulated in an Osprey-compatible format",
				EnvVars: []string{"ATKAFKA_OSPREY_COMPATIBLE"},
				Value:   false,
			},
			&cli.StringFlag{
				Name:    "plc-host",
				Usage:   "The host of the PLC directory you want to use for event metadata",
				EnvVars: []string{"ATKAFKA_PLC_HOST"},
			},
			&cli.StringSliceFlag{
				Name:    "watched-services",
				Usage:   "A list of ATProto services inside a user's DID document that you want to watch. Wildcards like *.bsky.network are allowed.",
				EnvVars: []string{"ATKAFKA_WATCHED_SERVICES"},
			},
			&cli.StringSliceFlag{
				Name:    "ignored-services",
				Usage:   "A list of ATProto services inside a user's DID document that you want to ignore. Wildcards like *.bsky.network are allowed.",
				EnvVars: []string{"ATKAFKA_IGNORED_SERVICES"},
			},
			&cli.StringSliceFlag{
				Name:    "watched-collections",
				Usage:   "A list of collections that you want to watch. Wildcards like *.bsky.app are allowed.",
				EnvVars: []string{"ATKAFKA_WATCHED_COLLECTIONS"},
			},
			&cli.StringSliceFlag{
				Name:    "ignored-collections",
				Usage:   "A list of collections that you want to ignore. Wildcards like *.bsky.app are allowed.",
				EnvVars: []string{"ATKAFKA_IGNORED_COLLECTIONS"},
			},
		},
		Action: func(cmd *cli.Context) error {
			ctx := context.Background()

			telemetry.StartMetrics(cmd)
			logger := telemetry.StartLogger(cmd)

			s, err := atkafka.NewServer(&atkafka.ServerArgs{
				RelayHost:          cmd.String("relay-host"),
				BootstrapServers:   cmd.StringSlice("bootstrap-servers"),
				OutputTopic:        cmd.String("output-topic"),
				OspreyCompat:       cmd.Bool("osprey-compatible"),
				PlcHost:            cmd.String("plc-host"),
				WatchedServices:    cmd.StringSlice("watched-services"),
				IgnoredServices:    cmd.StringSlice("ignored-services"),
				WatchedCollections: cmd.StringSlice("watched-collections"),
				IgnoredCollections: cmd.StringSlice("ignored-collections"),
				Logger:             logger,
			})
			if err != nil {
				return fmt.Errorf("failed to create new server: %w", err)
			}

			if err := s.Run(ctx); err != nil {
				return fmt.Errorf("error running server: %w", err)
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

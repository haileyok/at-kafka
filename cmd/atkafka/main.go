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
		},
		Commands: cli.Commands{
			&cli.Command{
				Name: "firehose",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "relay-host",
						Usage:   "Websocket host to subscribe to for events",
						Value:   "wss://bsky.network",
						EnvVars: []string{"ATKAFKA_RELAY_HOST"},
					},
					&cli.StringFlag{
						Name:    "plc-host",
						Usage:   "The host of the PLC directory you want to use for event metadata",
						EnvVars: []string{"ATKAFKA_PLC_HOST"},
					},
					&cli.StringFlag{
						Name:    "api-host",
						Usage:   "The API host for making XRPC calls. Recommended to use https://public.api.bsky.app",
						EnvVars: []string{"ATKAFKA_API_HOST"},
					},
					&cli.IntFlag{
						Name:    "tap-workers",
						Usage:   "Number of workers to process events from Tap",
						Value:   10,
						EnvVars: []string{"ATKAFKA_TAP_WORKERS"},
					},
					&cli.BoolFlag{
						Name:    "osprey-compatible",
						Usage:   "Whether or not events should be formulated in an Osprey-compatible format",
						EnvVars: []string{"ATKAFKA_OSPREY_COMPATIBLE"},
						Value:   false,
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
				Subcommands: cli.Commands{
					&cli.Command{
						Name: "relay",
						Action: func(cmd *cli.Context) error {
							ctx := context.Background()

							telemetry.StartMetrics(cmd)
							logger := telemetry.StartLogger(cmd)

							s, err := atkafka.NewServer(&atkafka.ServerArgs{
								RelayHost:          cmd.String("relay-host"),
								PlcHost:            cmd.String("plc-host"),
								ApiHost:            cmd.String("api-host"),
								BootstrapServers:   cmd.StringSlice("bootstrap-servers"),
								OutputTopic:        cmd.String("output-topic"),
								OspreyCompat:       cmd.Bool("osprey-compatible"),
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
					},
					&cli.Command{
						Name: "tap",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:    "tap-host",
								Usage:   "Tap host to subscribe to for events",
								Value:   "ws://localhost:2480",
								EnvVars: []string{"ATKAFKA_TAP_HOST"},
							},
							&cli.BoolFlag{
								Name:    "disable-acks",
								Usage:   "Set to `true` to disable sending of event acks to Tap. May result in the loss of events.",
								EnvVars: []string{"ATKAFKA_DISABLE_ACKS"},
							},
						},
						Action: func(cmd *cli.Context) error {

							ctx := context.Background()

							telemetry.StartMetrics(cmd)
							logger := telemetry.StartLogger(cmd)

							s, err := atkafka.NewServer(&atkafka.ServerArgs{
								TapHost:            cmd.String("tap-host"),
								DisableAcks:        cmd.Bool("disable-acks"),
								BootstrapServers:   cmd.StringSlice("bootstrap-servers"),
								OutputTopic:        cmd.String("output-topic"),
								WatchedCollections: cmd.StringSlice("watched-collections"),
								IgnoredCollections: cmd.StringSlice("ignored-collections"),
								Logger:             logger,
							})
							if err != nil {
								return fmt.Errorf("failed to create new server: %w", err)
							}

							if err := s.RunTapMode(ctx); err != nil {
								return fmt.Errorf("error running server: %w", err)
							}

							return nil
						},
					},
				},
			},
			&cli.Command{
				Name: "ozone-events",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "pds-host",
						EnvVars:  []string{"ATKAFKA_OZONE_PDS_HOST"},
						Required: true,
					},
					&cli.StringFlag{
						Name:     "identifier",
						EnvVars:  []string{"ATKAFKA_OZONE_IDENTIFIER"},
						Required: true,
					},
					&cli.StringFlag{
						Name:     "password",
						EnvVars:  []string{"ATKAFKA_OZONE_PASSWORD"},
						Required: true,
					},
					&cli.StringFlag{
						Name:     "labeler-did",
						EnvVars:  []string{"ATKAFKA_OZONE_LABELER_DID"},
						Required: true,
					},
				},
				Action: func(cmd *cli.Context) error {
					ctx := context.Background()

					telemetry.StartMetrics(cmd)
					logger := telemetry.StartLogger(cmd)

					s, err := atkafka.NewOzoneServer(&atkafka.OzoneServerArgs{
						Logger:           logger,
						BootstrapServers: cmd.StringSlice("bootstrap-servers"),
						OutputTopic:      cmd.String("output-topic"),
						OzoneClientArgs: &atkafka.OzoneClientArgs{
							OzonePdsHost:    cmd.String("pds-host"),
							OzoneIdentifier: cmd.String("identifier"),
							OzonePassword:   cmd.String("password"),
							OzoneLabelerDid: cmd.String("labeler-did"),
							Logger:          logger,
						},
					})
					if err != nil {
						return fmt.Errorf("failed to create new ozone server: %w", err)
					}

					if err := s.Run(ctx); err != nil {
						return fmt.Errorf("error running ozone server: %w", err)
					}

					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

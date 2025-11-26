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
				Value:   "wss://bsky.network",
				EnvVars: []string{"ATKAFKA_RELAY_HOST"},
			},
			&cli.StringSliceFlag{
				Name:     "bootstrap-servers",
				EnvVars:  []string{"ATKAFKA_BOOTSTRAP_SERVERS"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "output-topic",
				EnvVars:  []string{"ATKAFKA_OUTPUT_TOPIC"},
				Required: true,
			},
			&cli.BoolFlag{
				Name:    "osprey-compatible",
				EnvVars: []string{"ATKAFKA_OSPREY_COMPATIBLE"},
				Value:   false,
			},
			&cli.StringFlag{
				Name:    "plc-host",
				EnvVars: []string{"ATKAFKA_PLC_HOST"},
			},
		},
		Action: func(cmd *cli.Context) error {
			ctx := context.Background()

			telemetry.StartMetrics(cmd)
			logger := telemetry.StartLogger(cmd)

			s := atkafka.NewServer(&atkafka.ServerArgs{
				RelayHost:        cmd.String("relay-host"),
				BootstrapServers: cmd.StringSlice("bootstrap-servers"),
				OutputTopic:      cmd.String("output-topic"),
				OspreyCompat:     cmd.Bool("osprey-compatible"),
				PlcHost:          cmd.String("plc-host"),
				Logger:           logger,
			})

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

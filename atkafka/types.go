package atkafka

import (
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
)

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

type EventMetadata struct {
	DidDocument  identity.DIDDocument                `json:"didDocument,omitempty"`
	PdsHost      string                              `json:"pdsHost,omitempty"`
	Handle       string                              `json:"handle,omitempty"`
	DidCreatedAt string                              `json:"didCreatedAt,omitempty"`
	AccountAge   int64                               `json:"accountAge"`
	Profile      *bsky.ActorDefs_ProfileViewDetailed `json:"profile"`
}

type TapEvent struct {
	Id       int64             `json:"id"`
	Type     string            `json:"type"`
	Record   *TapEventRecord   `json:"record,omitempty"`
	Identity *TapEventIdentity `json:"identity,omitempty"`
}

type TapEventRecord struct {
	Live       bool            `json:"live"`
	Rev        string          `json:"rev"`
	Did        string          `json:"did"`
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	Action     string          `json:"action"`
	Cid        string          `json:"cid"`
	Record     *map[string]any `json:"record,omitempty"`
}

type TapEventIdentity struct {
	Did      string `json:"did"`
	Handle   string `json:"handle"`
	IsActive bool   `json:"isActive"`
	Status   string `json:"status"`
}

type TapAck struct {
	Type string `json:"type"`
	Id   int64  `json:"id"`
}

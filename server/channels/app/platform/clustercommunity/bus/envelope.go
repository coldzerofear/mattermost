// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

// Package bus holds the transport-agnostic primitives shared by the Redis and
// PostgreSQL cluster backends: wire envelope, handler dispatch table, node
// identity, and a failure counter. Keeping these in a sibling subpackage
// (rather than at the top-level clustercommunity package) breaks a potential
// import cycle: the top-level package imports concrete backends, and the
// backends import bus.
package bus

import (
	"encoding/json"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

// Envelope wraps a ClusterMessage with routing metadata so that receiving
// nodes can detect and drop their own messages (loopback prevention) and
// so that directed messages (target != "") can be filtered on the receiver.
//
// The envelope is JSON-encoded on the wire. JSON was chosen over gob for
// observability: operators can read messages directly from `redis-cli monitor`
// or `SELECT payload FROM cluster_messages`. The size overhead is negligible
// compared to the actual event data.
type Envelope struct {
	Sender    string                `json:"sender"`
	Target    string                `json:"target,omitempty"`
	Timestamp int64                 `json:"ts"`
	Message   *model.ClusterMessage `json:"msg"`
}

// Encode serializes an envelope around the given ClusterMessage.
// sender is the node ID of this process. target is "" for broadcasts.
func Encode(sender, target string, msg *model.ClusterMessage) ([]byte, error) {
	env := &Envelope{
		Sender:    sender,
		Target:    target,
		Timestamp: time.Now().UnixMilli(),
		Message:   msg,
	}
	return json.Marshal(env)
}

// Decode parses bytes from the wire back into an Envelope.
func Decode(data []byte) (*Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	return &env, nil
}

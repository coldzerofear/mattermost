// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package bus

import (
	"encoding/json"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	msg := &model.ClusterMessage{
		Event:    model.ClusterEventPublish,
		SendType: model.ClusterSendReliable,
		Props:    map[string]string{"k": "v"},
		Data:     []byte(`{"foo":"bar"}`),
	}

	data, err := Encode("node-a", "node-b", msg)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	env, err := Decode(data)
	require.NoError(t, err)
	require.Equal(t, "node-a", env.Sender)
	require.Equal(t, "node-b", env.Target)
	require.NotZero(t, env.Timestamp)
	require.Equal(t, model.ClusterEventPublish, env.Message.Event)
	require.Equal(t, map[string]string{"k": "v"}, env.Message.Props)
	require.Equal(t, []byte(`{"foo":"bar"}`), env.Message.Data)
}

func TestEnvelopeBroadcastOmitsTarget(t *testing.T) {
	// The wire format should not emit an empty Target field — it's easier
	// to spot directed vs. broadcast messages in `redis-cli monitor` if the
	// key is simply absent. We assert on the raw JSON rather than the
	// decoded struct.
	msg := &model.ClusterMessage{Event: "evt", Data: []byte("d")}
	data, err := Encode("node-a", "", msg)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, hasTarget := raw["target"]
	require.False(t, hasTarget, "broadcast envelope should omit target key, got %s", string(data))
}

func TestEnvelopeDecodeErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"not json", []byte("hello world")},
		{"truncated", []byte(`{"sender":"a","msg":`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode(tc.data)
			require.Error(t, err)
		})
	}
}

func TestEnvelopeDecodeAcceptsNilMessage(t *testing.T) {
	// Decode should not reject an envelope with a null message — the
	// receiver is responsible for skipping. This test pins the behavior
	// so we don't accidentally start rejecting it.
	env, err := Decode([]byte(`{"sender":"a","ts":1,"msg":null}`))
	require.NoError(t, err)
	require.Nil(t, env.Message)
}

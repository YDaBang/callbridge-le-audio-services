package control

import (
	"context"
	"testing"

	"callbridge.local/callbridge-sms-go/lecall/internal/gtbs"
	"callbridge.local/callbridge-sms-go/lecall/internal/protocol"
)

func TestServerConfigurationAndSnapshotEncoding(t *testing.T) {
	if _, err := NewServer("relative", 0, "00:11:22:33:44:55", func(_ context.Context, _ protocol.Message) error { return nil }, func() gtbs.Snapshot { return gtbs.Snapshot{} }); err == nil {
		t.Fatal("accepted relative socket")
	}
	server, err := NewServer("/tmp/callbridge-lecall-test.sock", 0, "00:11:22:33:44:55",
		func(_ context.Context, _ protocol.Message) error { return nil },
		func() gtbs.Snapshot { return gtbs.Snapshot{Sequence: 1} })
	if err != nil || server.device != "00:11:22:33:44:55" {
		t.Fatalf("server=%#v err=%v", server, err)
	}
}

func TestSnapshotMessagesRemainOneOrderedBatch(t *testing.T) {
	snapshot := gtbs.Snapshot{Sequence: 7, Calls: []gtbs.Call{
		{Index: 1, State: gtbs.StateActive, Token: 11},
		{Index: 2, State: gtbs.StateIncoming, Token: 12},
	}}
	messages := snapshotMessages("00:11:22:33:44:55", snapshot)
	if len(messages) != 2 || messages[0].Flags&protocol.FlagLast != 0 ||
		messages[1].Flags&protocol.FlagLast == 0 ||
		messages[0].Sequence != 7 || messages[1].Sequence != 7 {
		t.Fatalf("snapshot batch=%#v", messages)
	}
}

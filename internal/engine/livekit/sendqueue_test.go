package livekit

import (
	"encoding/binary"
	"testing"
)

func drainKeys(t *testing.T, q *sendScheduler) []string {
	t.Helper()
	var order []string
	for {
		pkt, ok := q.pop()
		if !ok {
			return order
		}
		order = append(order, queueKey(pkt))
	}
}

func TestSendSchedulerRoundRobinAcrossDestinations(t *testing.T) {
	q := newSendScheduler()
	for range 3 {
		_ = q.push(outboundPacket{to: "A"})
	}
	_ = q.push(outboundPacket{to: "B"})
	_ = q.push(outboundPacket{topic: announceTopic})
	got := drainKeys(t, q)
	want := []string{"A", "B", announceQueueKey, "A", "A"}
	if len(got) != len(want) {
		t.Fatalf("order = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %q, want %q", got, want)
		}
	}
	if q.length() != 0 || len(q.ring) != 0 || len(q.queues) != 0 {
		t.Fatalf("scheduler not empty after drain: total=%d ring=%v", q.length(), q.ring)
	}
}

func TestSendSchedulerKeepsOrderPerDestination(t *testing.T) {
	q := newSendScheduler()
	const n = 3 * compactAfter // long enough to exercise compaction
	push := func(to string, i int) {
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, uint32(i)) //nolint:gosec // small
		if err := q.push(outboundPacket{to: to, data: data}); err != nil {
			t.Fatal(err)
		}
	}
	next := map[string]int{}
	for i := range n {
		push("A", i)
		if i%3 == 0 {
			push("B", i/3)
		}
		// Interleave pops with pushes so queues stay non-empty and compact.
		if i%2 == 1 {
			pkt, _ := q.pop()
			got := int(binary.BigEndian.Uint32(pkt.data))
			if got != next[pkt.to] {
				t.Fatalf("destination %s: got packet %d, want %d", pkt.to, got, next[pkt.to])
			}
			next[pkt.to]++
		}
	}
	for {
		pkt, ok := q.pop()
		if !ok {
			break
		}
		got := int(binary.BigEndian.Uint32(pkt.data))
		if got != next[pkt.to] {
			t.Fatalf("destination %s: got packet %d, want %d", pkt.to, got, next[pkt.to])
		}
		next[pkt.to]++
	}
	if next["A"] != n || next["B"] != n/3 {
		t.Fatalf("delivered A=%d B=%d, want %d and %d", next["A"], next["B"], n, n/3)
	}
}

func TestSendSchedulerDropDataKeepsAnnounces(t *testing.T) {
	q := newSendScheduler()
	for range 10 {
		_ = q.push(outboundPacket{to: "A"})
		_ = q.push(outboundPacket{to: ""})
	}
	_ = q.push(outboundPacket{topic: announceTopic})
	_ = q.push(outboundPacket{to: "B"})
	if n := q.dropData(); n != 21 {
		t.Fatalf("dropData() = %d, want 21", n)
	}
	got := drainKeys(t, q)
	if len(got) != 1 || got[0] != announceQueueKey {
		t.Fatalf("left after drop = %q, want only the announce", got)
	}
	_ = q.push(outboundPacket{to: "A"})
	if got := drainKeys(t, q); len(got) != 1 || got[0] != "A" {
		t.Fatalf("scheduler unusable after drop: %q", got)
	}
}

func TestSendSchedulerBackPressureIsPerDestination(t *testing.T) {
	q := newSendScheduler()
	for range perDestQueueSoft {
		_ = q.push(outboundPacket{to: "A"})
	}
	if q.canSendTo("A") {
		t.Fatal("busy destination A reports free")
	}
	if !q.canSendTo("B") {
		t.Fatal("destination B blocked by A's queue")
	}
	for range perDestQueueSoft {
		_ = q.push(outboundPacket{to: "B"})
	}
	if q.length() < totalQueueSoft {
		t.Fatalf("total = %d, test expects the shared soft limit %d reached", q.length(), totalQueueSoft)
	}
	if !q.canSendTo("C") {
		t.Fatal("quiet destination C blocked although below its minimum share")
	}
	for range minDestShare {
		_ = q.push(outboundPacket{to: "C"})
	}
	if q.canSendTo("C") {
		t.Fatal("destination C still free beyond its share while the total is over the soft limit")
	}
	for range perDestQueueHard - perDestQueueSoft {
		if err := q.push(outboundPacket{to: "A"}); err != nil {
			t.Fatalf("push below hard cap: %v", err)
		}
	}
	if err := q.push(outboundPacket{to: "A"}); err == nil {
		t.Fatal("push beyond per-destination hard cap accepted")
	}
}

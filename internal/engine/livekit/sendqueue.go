package livekit

import "sync"

// sendScheduler keeps outbound packets in one FIFO per destination and hands
// them to the single publish worker round-robin across destinations.
//
// A LiveKit participant has one reliable data channel, so publishing stays
// serial. With one shared FIFO, a client downloading at full speed filled the
// queue with thousands of frames and every other client's frames (handshake,
// pings, small requests) waited behind them: in-memory, on a 40 Mbit/s
// uplink, a 16 KiB request beside three downloads took 2.8 s median instead
// of 4 ms. Round-robin bounds that wait to one frame per active destination.
//
// Packet order within one destination is preserved, which is all smux needs.
type sendScheduler struct {
	mu     sync.Mutex
	queues map[string]*destQueue
	ring   []string // destinations with queued packets, in service order
	next   int
	total  int
	wake   chan struct{}
}

type destQueue struct {
	pkts []outboundPacket
	head int
}

func (q *destQueue) len() int { return len(q.pkts) - q.head }

const (
	// perDestQueueSoft is where CanSend/CanSendTo start reporting
	// back-pressure for one destination. For a single destination (a client
	// pinned to its server, or a one-client server) this is exactly the old
	// shared-queue threshold.
	perDestQueueSoft = defaultSendQueueCapHard
	// perDestQueueHard rejects further packets for one destination.
	perDestQueueHard = defaultSendQueueSize
	// totalQueueSoft limits how much all destinations together may buffer
	// before busy destinations see back-pressure; a destination below
	// minDestShare can always enqueue, so a quiet client is never blocked by
	// busy ones.
	totalQueueSoft = 2 * defaultSendQueueCapHard
	totalQueueHard = 4 * defaultSendQueueSize
	minDestShare   = 256
	// announceQueueKey gives announces their own turn, so beacons never wait
	// behind a broadcast data flood.
	announceQueueKey = "\x00announce"
	// compactAfter bounds the dead prefix a long-busy queue may keep.
	compactAfter = 1024
)

func newSendScheduler() *sendScheduler {
	return &sendScheduler{
		queues: make(map[string]*destQueue),
		wake:   make(chan struct{}, 1),
	}
}

func queueKey(pkt outboundPacket) string {
	if pkt.topic == announceTopic {
		return announceQueueKey
	}
	return pkt.to
}

func (s *sendScheduler) push(pkt outboundPacket) error {
	key := queueKey(pkt)
	s.mu.Lock()
	q := s.queues[key]
	if q != nil && q.len() >= perDestQueueHard || s.total >= totalQueueHard {
		s.mu.Unlock()
		return ErrSendQueueFull
	}
	if q == nil {
		q = &destQueue{}
		s.queues[key] = q
		s.ring = append(s.ring, key)
	}
	q.pkts = append(q.pkts, pkt)
	s.total++
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}

// pop returns the next packet in round-robin order, or false when empty.
func (s *sendScheduler) pop() (outboundPacket, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.ring) > 0 {
		if s.next >= len(s.ring) {
			s.next = 0
		}
		key := s.ring[s.next]
		q := s.queues[key]
		if q == nil || q.len() == 0 {
			s.removeLocked(s.next, key)
			continue
		}
		pkt := q.pkts[q.head]
		q.pkts[q.head] = outboundPacket{}
		q.head++
		s.total--
		switch {
		case q.len() == 0:
			s.removeLocked(s.next, key)
		default:
			if q.head >= compactAfter && q.head*2 >= len(q.pkts) {
				n := copy(q.pkts, q.pkts[q.head:])
				clear(q.pkts[n:])
				q.pkts = q.pkts[:n]
				q.head = 0
			}
			s.next++
		}
		return pkt, true
	}
	return outboundPacket{}, false
}

// removeLocked drops an emptied destination; next keeps pointing at the
// destination that followed it.
func (s *sendScheduler) removeLocked(i int, key string) {
	delete(s.queues, key)
	s.ring = append(s.ring[:i], s.ring[i+1:]...)
}

func (s *sendScheduler) canSendTo(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	if q := s.queues[key]; q != nil {
		n = q.len()
	}
	if n >= perDestQueueSoft {
		return false
	}
	return s.total < totalQueueSoft || n < minDestShare
}

func (s *sendScheduler) length() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

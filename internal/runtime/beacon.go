package runtime

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/crypto"
)

// ServerBeaconInterval is how often a server on a room-directory carrier
// (LiveKit) announces itself to the room.
//
// The beacon does two jobs for the client. It names the participant that is
// the server, so the client can address its traffic to it and ignore frames
// from other clients sharing the room. And it proves that the client's own
// downstream subscription is live before the handshake: a LiveKit participant
// silently misses data sent during its first seconds in the room, so a
// SERVER_WELCOME sent too early is lost.
const ServerBeaconInterval = 3 * time.Second

const (
	beaconPlaintextLen = 16
	smuxVersion2       = 2
	smuxCmdUPD         = 4
	smuxUPDPayloadLen  = 8
)

// beaconDomain separates the identity tag from any other use of SHA-256 over
// the same string.
const beaconDomain = "olcrtc-server-beacon-v1:"

// EncodeServerBeacon builds the encrypted beacon for the server identity.
//
// The plaintext is a well-formed smux v2 window-update frame for stream 0
// whose 8-byte body is a tag derived from the identity. A legacy client that
// knows nothing about beacons pushes the record into its smux session like
// any other data frame; smux consumes a window update for a stream that does
// not exist and ignores it. The beacon therefore neither breaks nor confuses
// clients built before addressed mode, and still wakes their warm-up wait.
func EncodeServerBeacon(c *crypto.Cipher, identity string) ([]byte, error) {
	pt := make([]byte, beaconPlaintextLen)
	pt[0] = smuxVersion2
	pt[1] = smuxCmdUPD
	binary.LittleEndian.PutUint16(pt[2:4], smuxUPDPayloadLen)
	// pt[4:8] is stream id 0.
	tag := beaconTag(identity)
	copy(pt[8:], tag[:])
	out, err := c.Encrypt(pt)
	if err != nil {
		return nil, fmt.Errorf("encrypt beacon: %w", err)
	}
	return out, nil
}

// VerifyServerBeacon reports whether data is a beacon encrypted with our key
// and issued for sender. Binding the tag to the identity stops another room
// participant from replaying a captured beacon under its own name.
func VerifyServerBeacon(c *crypto.Cipher, sender string, data []byte) bool {
	if sender == "" || c == nil {
		return false
	}
	pt, err := c.Decrypt(data)
	if err != nil || len(pt) != beaconPlaintextLen {
		return false
	}
	if pt[0] != smuxVersion2 || pt[1] != smuxCmdUPD ||
		binary.LittleEndian.Uint16(pt[2:4]) != smuxUPDPayloadLen ||
		binary.LittleEndian.Uint32(pt[4:8]) != 0 {
		return false
	}
	tag := beaconTag(sender)
	return subtle.ConstantTimeCompare(pt[8:], tag[:]) == 1
}

func beaconTag(identity string) [8]byte {
	sum := sha256.Sum256([]byte(beaconDomain + identity))
	var tag [8]byte
	copy(tag[:], sum[:8])
	return tag
}

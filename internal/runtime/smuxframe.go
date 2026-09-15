package runtime

import "encoding/binary"

// smux v2 frame layout: version(1) cmd(1) length(2, LE) stream id(4, LE).
// Every muxconn record carries exactly one smux frame, so a decrypted record
// can be classified without a smux session.
const (
	smuxHeaderLen  = 8
	smuxCmdSYN     = 0
	smuxCmdFIN     = 1
	// smuxControlSID is the id of the first stream a smux client opens: the
	// client counter starts at 1 and is advanced by 2 before use (smux
	// session.go OpenStream), so the handshake/control stream is 3 and
	// tunnels follow as 5, 7, ...
	smuxControlSID = 3
)

// IsSmuxSessionStart reports whether a decrypted record is the frame a client
// sends first on a fresh session: SYN for its first stream (the handshake
// stream).
// Any other frame from a participant without a session is the tail of a
// session the server no longer has; opening a session from it would accept a
// tunnel stream as the handshake stream.
func IsSmuxSessionStart(pt []byte) bool {
	return len(pt) == smuxHeaderLen &&
		pt[0] == smuxVersion2 && pt[1] == smuxCmdSYN &&
		binary.LittleEndian.Uint16(pt[2:4]) == 0 &&
		binary.LittleEndian.Uint32(pt[4:8]) == smuxControlSID
}

// SmuxControlFIN returns the plaintext smux FIN for the client's handshake
// stream. Sent to a client
// whose session the server has dropped, it ends the client's control stream
// with EOF, and the client re-establishes its session at once instead of
// waiting for missed pongs.
func SmuxControlFIN() []byte {
	pt := make([]byte, smuxHeaderLen)
	pt[0] = smuxVersion2
	pt[1] = smuxCmdFIN
	binary.LittleEndian.PutUint32(pt[4:8], smuxControlSID)
	return pt
}

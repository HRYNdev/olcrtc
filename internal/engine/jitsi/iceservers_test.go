package jitsi

import (
	"reflect"
	"testing"

	"github.com/pion/webrtc/v4"
)

// ai-generated: extracted repeated ICE server test strings for lint compliance.
const (
	testSTUNURL          = "stun:stun.example.com:3478"
	testSTUNEmptyPort    = "stun:stun.example.com:"
	testSTUNSURL         = "stuns:stun.example.com:5349"
	testTURNURL          = "turn:turn.example.com:3478"
	testTURNUDPURL       = "turn:turn.example.com:3478?transport=udp"
	testTURNUDPEmptyPort = "turn:turn.example.com:?transport=udp"
	testTURNSURL         = "turns:turn.example.com:5349"
	testTURNSTCPURL      = "turns:turn.example.com:5349?transport=tcp"
	testIPv6STUNURL      = "stun:[2001:db8::1]:3478"
	testTURNUsername     = "user"
	testTURNCredential   = "secret"
)

func TestNormaliseICEServerURL(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string // empty means the URL must be rejected
	}{
		// Already canonical URLs pass through unchanged.
		{"stun explicit port", testSTUNURL, testSTUNURL},
		{"turn udp", testTURNUDPURL, testTURNUDPURL},
		{"turn tcp", "turn:turn.example.com:443?transport=tcp", "turn:turn.example.com:443?transport=tcp"},
		{"turns tcp", testTURNSTCPURL, testTURNSTCPURL},

		// The XEP-0215 no-port breakage: empty port after the colon.
		{"stun empty port", testSTUNEmptyPort, testSTUNURL},
		{"turn empty port transport", testTURNUDPEmptyPort, testTURNUDPURL},
		{"turns empty port", "turns:turn.example.com:", testTURNSURL},

		// Missing port entirely: pion itself defaults these, keep parity.
		{"stun no port", "stun:stun.example.com", testSTUNURL},
		{"stuns no port", "stuns:stun.example.com", testSTUNSURL},
		{"turn no port with transport", "turn:turn.example.com?transport=udp", testTURNUDPURL},
		{"turns no port", "turns:turn.example.com", testTURNSURL},

		// Transport handling: empty/unknown transports are stripped so pion
		// applies the scheme default instead of rejecting the URL.
		{"turn empty transport", "turn:turn.example.com:3478?transport=", testTURNURL},
		{"turn no query", testTURNURL, testTURNURL},
		{"turn unknown transport", "turn:turn.example.com:443?transport=ssltcp", "turn:turn.example.com:443"},
		{"turn uppercase transport", "turn:turn.example.com:3478?transport=UDP", testTURNUDPURL},
		{"stun query stripped", "stun:stun.example.com:3478?transport=udp", testSTUNURL},

		// IPv6 hosts keep their brackets through host:port splitting.
		{"ipv6 explicit port", testIPv6STUNURL, testIPv6STUNURL},
		{"ipv6 no port", "stun:[2001:db8::1]", "stun:[2001:db8::1]:3478"},
		{"ipv6 turn", "turn:[2001:db8::1]:3478?transport=tcp", "turn:[2001:db8::1]:3478?transport=tcp"},

		// Scheme and whitespace normalisation.
		{"uppercase scheme", "STUN:stun.example.com:3478", testSTUNURL},
		{"surrounding whitespace", "  stun:stun.example.com:3478  ", testSTUNURL},

		// Truly unsalvageable entries are rejected.
		{"empty string", "", ""},
		{"whitespace only", "   ", ""},
		{"no scheme", "stun.example.com:3478", ""},
		{"unknown scheme", "http://example.com", ""},
		{"authority form", "stun://stun.example.com:3478", ""},
		{"missing host", "stun::3478", ""},
		{"only colon", "stun::", ""},
		{"non numeric port", "stun:stun.example.com:notaport", ""},
		{"port out of range", "stun:stun.example.com:70000", ""},
		{"port zero", "stun:stun.example.com:0", ""},
		{"negative port", "stun:stun.example.com:-1", ""},
		{"unbracketed ipv6", "stun:2001:db8::1", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := normaliseICEServerURL(tc.raw)
			if tc.want == "" {
				if ok {
					t.Fatalf("normaliseICEServerURL(%q) = %q, true; want rejection", tc.raw, got)
				}
				return
			}
			if !ok || got != tc.want {
				t.Fatalf("normaliseICEServerURL(%q) = %q, %v; want %q, true", tc.raw, got, ok, tc.want)
			}
		})
	}
}

func TestNormaliseICEServers(t *testing.T) {
	in := []webrtc.ICEServer{
		{URLs: []string{testSTUNEmptyPort}}, // salvage: default port
		{
			// Mixed URLs: the broken one is fixed, the good one kept.
			URLs:       []string{testTURNUDPEmptyPort, "turns:turn.example.com:443?transport=tcp"},
			Username:   testTURNUsername,
			Credential: testTURNCredential,
		},
		{URLs: []string{"stun:bad.example.com:notaport"}}, // dropped: unusable
		{URLs: []string{}}, // dropped: nothing to keep
	}
	want := []webrtc.ICEServer{
		{URLs: []string{testSTUNURL}},
		{
			URLs:       []string{testTURNUDPURL, "turns:turn.example.com:443?transport=tcp"},
			Username:   testTURNUsername,
			Credential: testTURNCredential,
		},
	}

	got := normaliseICEServers(in)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normaliseICEServers mismatch:\n got: %+v\nwant: %+v", got, want)
	}
}

// TestNormaliseICEServersTURNCredentialGating locks down the pion credential
// rule: TURN/TURNS URLs on servers without a non-empty username and a
// non-empty string credential are dropped (pion fails NewPeerConnection with
// "no turn server credentials" otherwise), while STUN/STUNS URLs on the same
// server survive because they need no credentials.
func TestNormaliseICEServersTURNCredentialGating(t *testing.T) {
	tests := []struct {
		name string
		in   []webrtc.ICEServer
		want []webrtc.ICEServer
	}{
		{
			name: "turn without credentials dropped",
			in: []webrtc.ICEServer{
				{URLs: []string{testTURNUDPURL}},
			},
			want: []webrtc.ICEServer{},
		},
		{
			name: "turns without credentials dropped",
			in: []webrtc.ICEServer{
				{URLs: []string{testTURNSTCPURL}},
			},
			want: []webrtc.ICEServer{},
		},
		{
			name: "turn username only dropped",
			in: []webrtc.ICEServer{
				{URLs: []string{testTURNURL}, Username: testTURNUsername},
			},
			want: []webrtc.ICEServer{},
		},
		{
			name: "turn credential only dropped",
			in: []webrtc.ICEServer{
				{URLs: []string{testTURNURL}, Credential: testTURNCredential},
			},
			want: []webrtc.ICEServer{},
		},
		{
			name: "turn empty string credential dropped",
			in: []webrtc.ICEServer{
				{URLs: []string{testTURNURL}, Username: testTURNUsername, Credential: ""},
			},
			want: []webrtc.ICEServer{},
		},
		{
			name: "turn non-string credential dropped",
			in: []webrtc.ICEServer{
				{URLs: []string{testTURNURL}, Username: testTURNUsername, Credential: 42},
			},
			want: []webrtc.ICEServer{},
		},
		{
			name: "mixed stun and credential-less turn keeps stun",
			in: []webrtc.ICEServer{
				{URLs: []string{testSTUNURL, testTURNUDPURL}},
			},
			want: []webrtc.ICEServer{
				{URLs: []string{testSTUNURL}},
			},
		},
		{
			name: "valid turn credentials preserved",
			in: []webrtc.ICEServer{
				{
					URLs:       []string{testTURNUDPURL, testTURNSTCPURL},
					Username:   testTURNUsername,
					Credential: testTURNCredential,
				},
			},
			want: []webrtc.ICEServer{
				{
					URLs:       []string{testTURNUDPURL, testTURNSTCPURL},
					Username:   testTURNUsername,
					Credential: testTURNCredential,
				},
			},
		},
		{
			name: "stun unaffected by missing credentials",
			in: []webrtc.ICEServer{
				{URLs: []string{testSTUNURL, testSTUNSURL}},
			},
			want: []webrtc.ICEServer{
				{URLs: []string{testSTUNURL, testSTUNSURL}},
			},
		},
		{
			name: "credential-less turn does not poison other servers",
			in: []webrtc.ICEServer{
				{URLs: []string{"turn:anon.example.com:3478?transport=udp"}},
				{URLs: []string{testSTUNEmptyPort}},
				{
					URLs:       []string{testTURNUDPEmptyPort},
					Username:   testTURNUsername,
					Credential: testTURNCredential,
				},
			},
			want: []webrtc.ICEServer{
				{URLs: []string{testSTUNURL}},
				{
					URLs:       []string{testTURNUDPURL},
					Username:   testTURNUsername,
					Credential: testTURNCredential,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := normaliseICEServers(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("normaliseICEServers mismatch:\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

func TestNormaliseICEServersEmpty(t *testing.T) {
	if got := normaliseICEServers(nil); len(got) != 0 {
		t.Fatalf("normaliseICEServers(nil) = %+v, want empty", got)
	}
}

// TestNormalisedICEServersAcceptedByPion feeds normalised servers through
// webrtc.NewPeerConnection, the exact call that rejected the raw URLs with
// "InvalidAccessError: invalid port" on deployments whose XEP-0215 disco
// omits the port attribute (e.g. meet.ffmuc.net), and with
// "InvalidAccessError: no turn server credentials" when a TURN server is
// advertised without credentials.
func TestNormalisedICEServersAcceptedByPion(t *testing.T) {
	raw := []webrtc.ICEServer{
		{URLs: []string{testSTUNEmptyPort}},
		{URLs: []string{"stun:[2001:db8::1]"}},
		{
			URLs:       []string{testTURNUDPEmptyPort, "turns:turn.example.com:?transport="},
			Username:   testTURNUsername,
			Credential: testTURNCredential,
		},
	}
	// Bad TURN entries pion would reject outright: no credentials, partial
	// credentials, and a STUN+TURN mix where only the TURN URL must go.
	bad := []webrtc.ICEServer{
		{URLs: []string{"turn:anon.example.com:3478?transport=udp"}},
		{URLs: []string{"turns:anon.example.com:5349?transport=tcp"}, Username: "user"},
		{URLs: []string{"stun:keep.example.com:3478", "turn:drop.example.com:3478"}},
	}

	all := make([]webrtc.ICEServer, 0, len(raw)+len(bad))
	all = append(all, raw...)
	all = append(all, bad...)
	normalised := normaliseICEServers(all)
	// raw survives intact, the first two bad servers vanish, and the third
	// keeps only its STUN URL.
	wantLen := len(raw) + 1
	if len(normalised) != wantLen {
		t.Fatalf("expected %d servers after normalisation, got %d: %+v", wantLen, len(normalised), normalised)
	}
	last := normalised[len(normalised)-1]
	if len(last.URLs) != 1 || last.URLs[0] != "stun:keep.example.com:3478" {
		t.Fatalf("expected mixed server to keep only its STUN URL, got %+v", last)
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: normalised})
	if err != nil {
		t.Fatalf("pion rejected normalised ICE servers: %v\nservers: %+v", err, normalised)
	}
	if closeErr := pc.Close(); closeErr != nil {
		t.Fatalf("close peer connection: %v", closeErr)
	}
}

package cli

import (
	"bytes"
	"compress/flate"
	"encoding/base64"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/ugorji/go/codec"
)

func TestPreferTCPRanker(t *testing.T) {
	tcpAddr := ma.StringCast("/ip4/127.0.0.1/tcp/4001")
	udpAddr := ma.StringCast("/ip4/127.0.0.1/udp/4001/quic-v1")

	delays := PreferTCPRanker([]ma.Multiaddr{tcpAddr, udpAddr})
	if len(delays) != 2 {
		t.Fatalf("expected 2 delays, got %d", len(delays))
	}

	if delays[0] != (network.AddrDelay{Addr: tcpAddr, Delay: 0}) {
		t.Fatalf("unexpected tcp delay: %#v", delays[0])
	}

	if delays[1] != (network.AddrDelay{Addr: udpAddr, Delay: 500 * time.Millisecond}) {
		t.Fatalf("unexpected udp delay: %#v", delays[1])
	}
}

func TestFilterFunctions(t *testing.T) {
	tcpAddr := ma.StringCast("/ip4/127.0.0.1/tcp/4001")
	wssAddr := ma.StringCast("/dns/example.com/tcp/443/wss")

	if !FilterTCP(tcpAddr) {
		t.Fatal("expected tcp addr to pass FilterTCP")
	}
	if FilterTCP(wssAddr) {
		t.Fatal("expected wss addr to fail FilterTCP")
	}

	if !FilterWSS(wssAddr) {
		t.Fatal("expected wss addr to pass FilterWSS")
	}
	if FilterWSS(tcpAddr) {
		t.Fatal("expected tcp addr to fail FilterWSS")
	}
}

func TestGetCircuitAddr(t *testing.T) {
	base := ma.StringCast("/ip4/127.0.0.1/tcp/4001")
	id := peer.ID("test-peer-id")

	got := GetCircuitAddr(base, id)
	want := "/ip4/127.0.0.1/tcp/4001/p2p-circuit/p2p/test-peer-id"
	if got.String() != want {
		t.Fatalf("unexpected circuit addr: got %q, want %q", got.String(), want)
	}
}

func TestEncodeDecodePeerAddrinfoRoundTrip(t *testing.T) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}

	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("peer id from key: %v", err)
	}

	addrs := []ma.Multiaddr{
		ma.StringCast("/ip4/127.0.0.1/tcp/4001"),
		ma.StringCast("/dns/example.com/tcp/443/wss"),
	}

	encoded, err := encodePeerAddrinfo(id, addrs)
	if err != nil {
		t.Fatalf("encodePeerAddrinfo: %v", err)
	}

	decoded, err := decodePeerAddrinfo(encoded)
	if err != nil {
		t.Fatalf("decodePeerAddrinfo: %v", err)
	}

	if decoded.ID != id {
		t.Fatalf("unexpected id: got %q, want %q", decoded.ID, id)
	}
	if len(decoded.Addrs) != len(addrs) {
		t.Fatalf("unexpected addr length: got %d, want %d", len(decoded.Addrs), len(addrs))
	}
	for i := range addrs {
		if decoded.Addrs[i].String() != addrs[i].String() {
			t.Fatalf("unexpected addr[%d]: got %q, want %q", i, decoded.Addrs[i], addrs[i])
		}
	}
}

func TestDecodePeerAddrinfoRejectsInvalidBase64(t *testing.T) {
	if _, err := decodePeerAddrinfo("%%%not-base64%%%"); err == nil {
		t.Fatal("expected decodePeerAddrinfo to fail for invalid base64")
	}
}

func TestDecodePeerAddrinfoRejectsInvalidMultiaddrBytes(t *testing.T) {
	encoded, err := encodeCompactForTest(CompactAddrInfo{
		ID:    []byte("peer"),
		Addrs: [][]byte{{0x01, 0x02, 0x03}},
	})
	if err != nil {
		t.Fatalf("encodeCompactForTest: %v", err)
	}

	if _, err = decodePeerAddrinfo(encoded); err == nil {
		t.Fatal("expected decodePeerAddrinfo to fail for invalid multiaddr bytes")
	}
}

func encodeCompactForTest(compact CompactAddrInfo) (string, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		return "", err
	}

	var h codec.BincHandle
	h.StructToArray = true
	if err = codec.NewEncoder(w, &h).Encode(compact); err != nil {
		return "", err
	}
	if err = w.Flush(); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(buf.Bytes()), nil
}

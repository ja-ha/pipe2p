package cli

import (
	"bytes"
	"compress/flate"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/ugorji/go/codec"
)

func NewNode(customRelay string) (host.Host, error) {
	var staticRelays []peer.AddrInfo

	if customRelay != "" {
		staticRelay, err := peer.AddrInfoFromString(customRelay)
		if err != nil {
			return nil, err
		}
		staticRelays = append(staticRelays, *staticRelay)
	} else {
		staticRelays = dht.GetDefaultBootstrapPeerAddrInfos()
	}

	opts := []libp2p.Option{
		libp2p.EnableAutoRelayWithStaticRelays(staticRelays),
		libp2p.EnableHolePunching(),
		libp2p.ForceReachabilityPrivate(),
		libp2p.SwarmOpts(swarm.WithDialRanker(PreferTCPRanker)),
	}

	node, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func PreferTCPRanker(addrs []ma.Multiaddr) []network.AddrDelay {
	var delays []network.AddrDelay
	for _, a := range addrs {
		delay := time.Duration(0)

		if _, err := a.ValueForProtocol(ma.P_UDP); err == nil {
			delay = 500 * time.Millisecond
		}

		delays = append(delays, network.AddrDelay{Addr: a, Delay: delay})
	}
	return delays
}

func FilterTCP(a ma.Multiaddr) bool {
	_, err := a.ValueForProtocol(ma.P_TCP)
	return err == nil
}

func FilterWSS(a ma.Multiaddr) bool {
	_, err := a.ValueForProtocol(ma.P_WSS)
	return err == nil
}

func GetCircuitAddr(a ma.Multiaddr, id peer.ID) ma.Multiaddr {
	return a.Encapsulate(ma.StringCast(
		fmt.Sprintf("/p2p-circuit/p2p/%s", id)))
}

type CompactAddrInfo struct {
	ID    []byte   `codec:"i"`
	Addrs [][]byte `codec:"a"`
}

func encodePeerAddrinfo(id peer.ID, addr []ma.Multiaddr) (string, error) {
	bID, err := id.MarshalBinary()
	if err != nil {
		return "", err
	}

	bAddrs := make([][]byte, len(addr))
	for i, a := range addr {
		bAddrs[i] = a.Bytes()
	}

	compact := CompactAddrInfo{
		ID:    bID,
		Addrs: bAddrs,
	}

	var buf bytes.Buffer
	flateWriter, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		return "", err
	}

	// setup binc encoder
	var h codec.BincHandle
	h.StructToArray = true

	enc := codec.NewEncoder(flateWriter, &h)
	if err = enc.Encode(compact); err != nil {
		return "", err
	}

	// flush compressor
	if err = flateWriter.Flush(); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(buf.Bytes()), nil
}

func decodePeerAddrinfo(encoded string) (*peer.AddrInfo, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}

	deflateReader := flate.NewReader(bytes.NewReader(decoded))
	defer deflateReader.Close()

	var h codec.BincHandle
	h.StructToArray = true

	var compact CompactAddrInfo
	dec := codec.NewDecoder(deflateReader, &h)
	if err = dec.Decode(&compact); err != nil {
		return nil, err
	}

	p := peer.AddrInfo{
		ID:    peer.ID(compact.ID),
		Addrs: make([]ma.Multiaddr, len(compact.Addrs)),
	}

	var a ma.Multiaddr
	for i, b := range compact.Addrs {
		a, err = ma.NewMultiaddrBytes(b)
		if err != nil {
			return nil, err
		}
		p.Addrs[i] = a
	}

	return &p, nil
}

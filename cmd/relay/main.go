package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
)

func main() {
	keyFile := flag.String("keyfile", "relay.key", "Location of the keyfile")
	port := flag.Int("port", 4001, "Listen port for websocket")
	domain := flag.String("domain", "", "Public domain name for WSS (e.g. relay.example.com)")
	flag.Parse()

	if domain == nil || *domain == "" {
		log.Fatalf("-domain is required")
	}

	privateKey, err := loadOrGenerateKey(*keyFile)
	if err != nil {
		log.Fatalf("Failed to load or generate key: %v", err)
	}

	h, err := libp2p.New(
		libp2p.Identity(privateKey),
		libp2p.EnableNATService(),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip6/::1/tcp/%d/ws", *port),
		),
		libp2p.ShareTCPListener(),
		libp2p.ForceReachabilityPublic(),
		libp2p.AddrsFactory(func(addrs []ma.Multiaddr) []ma.Multiaddr {
			var out []ma.Multiaddr

			// inject the public wss address
			wssStr := fmt.Sprintf("/dns/%s/tcp/443/wss", *domain)
			wssAddr, err := ma.NewMultiaddr(wssStr)
			if err == nil {
				out = []ma.Multiaddr{wssAddr}
			} else {
				log.Printf("Failed to parse WSS multiaddr: %v", err)
			}

			return out
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create libp2p host: %v", err)
	}
	defer h.Close()

	// start the relay
	r, err := relay.New(h, relay.WithInfiniteLimits())
	if err != nil {
		log.Fatalf("Failed to instantiate relay: %v", err)
	}
	defer r.Close()

	fmt.Println("libp2p Relay Node Started")
	fmt.Printf("PeerID: %s\n", h.ID().String())
	fmt.Println("Connect to this relay using the following addresses:")
	for _, addr := range h.Addrs() {
		fmt.Printf("   %s/p2p/%s\n", addr.String(), h.ID().String())
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	fmt.Println("\nShutting down relay...")
}

func loadOrGenerateKey(path string) (crypto.PrivKey, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		privateKey, err := crypto.UnmarshalPrivateKey(b)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
		}
		return privateKey, nil
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	fmt.Println("No existing key found. Generating a new identity...")
	privateKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	b, err = crypto.MarshalPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	err = os.WriteFile(path, b, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to write key to file: %w", err)
	}

	return privateKey, nil
}

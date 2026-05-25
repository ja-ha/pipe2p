package cli

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"pipe2p/internal/transfer"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/spf13/cobra"
)

var compress bool
var compressLvl int

var sendCmd = &cobra.Command{
	Use:   "send [file_path]",
	Short: "Send a file or stdin stream to a peer",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		log.SetOutput(os.Stderr)

		transfer.Verbose = verbose
		transfer.Compress = compress
		transfer.CompressLvl = compressLvl

		var source io.Reader
		var name string
		var size int64 = -1

		// determine data source (file / stdin)
		if len(args) == 1 {
			filePath := args[0]
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatalf("Could not open file: %v", err)
			}
			defer file.Close()

			stat, _ := file.Stat()
			source = file
			name = stat.Name()
			size = stat.Size()
		} else {
			stat, _ := os.Stdin.Stat()
			if (stat.Mode() & os.ModeCharDevice) != 0 {
				log.Fatalf("No file specified and no data piped to stdin.")
			}
			source = os.Stdin
			name = ""
		}

		node, err := NewNode(customRelay)
		if err != nil {
			log.Fatalf("Failed to start node: %v", err)
		}
		defer node.Close()

		// Request a Relay V2 reservation
		reservation := makeReservation(node)

		// Choose a relay multiaddrs
		// Prefer tcp
		reserverationAddrs := ma.FilterAddrs(reservation.Addrs, FilterTCP)
		if len(reserverationAddrs) == 0 {
			reserverationAddrs = reservation.Addrs
		}

		// Prefer websocket
		wssReserverationAddrs := ma.FilterAddrs(reserverationAddrs, FilterWSS)
		if len(wssReserverationAddrs) != 0 {
			reserverationAddrs = wssReserverationAddrs
		}

		var receiverDialedAddrs []ma.Multiaddr
		var errs []error
		var circuitListenAddr ma.Multiaddr
		for _, reservationAddr := range reserverationAddrs {
			circuitListenAddr = GetCircuitAddr(reservationAddr, node.ID())

			if err = node.Network().Listen(circuitListenAddr); err != nil {
				errs = append(errs, err)
			} else {
				receiverDialedAddrs = append(receiverDialedAddrs, circuitListenAddr)
			}
		}
		if len(errs) == len(receiverDialedAddrs) {
			log.Fatalln("Listening on all circuit addresses failed")
			return
		}

		peerCode, err := encodePeerAddrinfo(node.ID(), receiverDialedAddrs)
		if err != nil {
			log.Fatalf("Failed to encode peer code: %v", err)
		}

		// setup done, register stream handler
		done := make(chan struct{})
		connected := make(chan struct{})
		node.SetStreamHandler(transfer.ProtocolID, func(s network.Stream) {
			transfer.HandleIncomingStream(s, source, name, size, done, connected)
		})

		if _, err = fmt.Fprintf(os.Stderr, "To receive run:\npipe2p receive %s\n\n", peerCode); err != nil {
			log.Fatalf("Error during stdout write: %s\n", err)
		}

		ctx, cancel := context.WithDeadline(context.Background(), reservation.Expiration)
		defer cancel()

		// Blocks until transfer completes or relay reservation runs out without a connected receiver
		select {
		case <-connected:
			if verbose {
				log.Println("Connected to peer")
			}
			<-done
			if verbose {
				log.Println("Disconnected from peer")
			}
		case <-ctx.Done():
			// Reservation expired before connection
		}
	},
}

func makeReservation(n host.Host) client.Reservation {
	var reservation client.Reservation

	if customRelay != "" {
		addrInfo, err := peer.AddrInfoFromString(customRelay)
		if err != nil {
			log.Fatalf("Failed to parse custom relay: %v", err)
		}

		_reservation, err := client.Reserve(context.Background(), n, *addrInfo)
		if err != nil {
			log.Fatalf("Failed to reserve slot on custom relay: %v", err)
		}

		reservation = *_reservation
	} else {
		resChan := make(chan client.Reservation)
		cancelContext, cancel := context.WithCancel(context.Background())

		for _, addrInfo := range dht.GetDefaultBootstrapPeerAddrInfos() {
			go func(c chan client.Reservation, ctx context.Context, host host.Host, addrInfo peer.AddrInfo) {
				r, err := client.Reserve(ctx, host, addrInfo)
				if err == nil {
					select {
					case c <- *r:
					case <-ctx.Done():
					}
				}
			}(resChan, cancelContext, n, addrInfo)
		}

		reservation = <-resChan
		cancel()
	}
	return reservation
}

func init() {
	rootCmd.AddCommand(sendCmd)
	sendCmd.Flags().BoolVarP(&compress, "compress", "c", false, "Compress data using zstd")
	sendCmd.Flags().IntVarP(&compressLvl, "level", "l", 8, "Compression level for zstd data compression")
}

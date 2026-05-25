package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"pipe2p/internal/transfer"
	"sync"

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
				_, _ = fmt.Fprintf(os.Stderr, "Could not open file: %v\n", err)
				os.Exit(1)
			}
			defer file.Close()

			stat, _ := file.Stat()
			source = file
			name = stat.Name()
			size = stat.Size()
		} else {
			stat, _ := os.Stdin.Stat()
			if (stat.Mode() & os.ModeCharDevice) != 0 {
				_, _ = fmt.Fprintln(os.Stderr, "No file specified and no data piped to stdin.")
				os.Exit(1)
			}
			source = os.Stdin
			name = ""
		}

		node, err := NewNode(customRelay)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to start node: %v\n", err)
			os.Exit(1)
		}
		defer node.Close()

		// Request a Relay V2 reservation
		reservation := makeReservation(node)

		// choose a relay multiaddrs
		// prefer tcp
		reserverationAddrs := ma.FilterAddrs(reservation.Addrs, FilterTCP)
		if len(reserverationAddrs) == 0 {
			reserverationAddrs = reservation.Addrs
		}

		// prefer websocket
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
			_, _ = fmt.Fprintln(os.Stderr, "Listening on all circuit addresses failed")
			os.Exit(1)
		}

		peerCode, err := encodePeerAddrinfo(node.ID(), receiverDialedAddrs)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to encode peer code: %v\n", err)
			os.Exit(1)
		}

		// setup done, register stream handler
		done := make(chan struct{})
		connected := make(chan struct{})
		node.SetStreamHandler(transfer.ProtocolID, func(s network.Stream) {
			transfer.HandleIncomingStream(s, source, name, size, done, connected)
		})

		_, err = fmt.Fprintf(os.Stderr, "To receive run:\npipe2p receive %s\n\n", peerCode)
		if err != nil {
			os.Exit(1)
		}

		ctx, cancel := context.WithDeadline(context.Background(), reservation.Expiration)
		defer cancel()

		// Blocks until transfer completes or relay reservation runs out without a connected receiver
		select {
		case <-connected:
			if verbose {
				_, _ = fmt.Fprintln(os.Stderr, "Connected to peer")
			}
			<-done
			if verbose {
				_, _ = fmt.Fprintln(os.Stderr, "Disconnected from peer")
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
			_, _ = fmt.Fprintf(os.Stderr, "Failed to parse custom relay: %v\n", err)
			os.Exit(1)
		}

		_reservation, err := client.Reserve(context.Background(), n, *addrInfo)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to reserve slot on custom relay: %v\n", err)
			os.Exit(1)
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

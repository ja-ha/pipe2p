package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ja-ha/pipe2p/internal/transfer"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

var receiveYesOverwrite bool
var holePunchTimeout time.Duration

var receiveCmd = &cobra.Command{
	Use:   "receive [peer_code]",
	Short: "Receive a file from a peer",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		transfer.Verbose = verbose
		transfer.AutoAcceptOverwrite = receiveYesOverwrite

		peerCode := args[0]
		p, err := decodePeerAddrinfo(peerCode)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to decode peer code: %s\n", err)
			os.Exit(1)
		}

		if len(p.Addrs) == 0 {
			_, _ = fmt.Fprintln(os.Stderr, "No multiaddress in peer record")
			os.Exit(1)
		}

		node, err := NewNode("")
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to start node: %s\n", err)
			os.Exit(1)
		}
		defer node.Close()

		// hole punch timeout logic setup
		holePunchedCh := make(chan struct{}, 1)
		notifiee := &network.NotifyBundle{
			ConnectedF: func(n network.Network, c network.Conn) {
				if c.RemotePeer() == p.ID {
					if _, err := c.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT); err != nil {
						// non-blocking send
						select {
						case holePunchedCh <- struct{}{}:
						default:
						}
					}
				}
			},
		}
		node.Network().Notify(notifiee)

		conCtx, conCtxCancel := context.WithCancel(context.Background())
		resultChan := make(chan error, len(p.Addrs))
		for _, addr := range p.Addrs {
			addr = GetCircuitAddr(addr, p.ID)

			go func(a ma.Multiaddr) {
				ai, err := peer.AddrInfoFromP2pAddr(addr)
				if err != nil {
					resultChan <- err
					return
				}

				if verbose {
					_, _ = fmt.Fprintf(os.Stderr, "Trying connection via %s\n", a)
				}

				err = node.Connect(conCtx, *ai)
				select {
				case resultChan <- err:
				case <-conCtx.Done():
				}
			}(addr)
		}

		// wait for the first success, or until all attempts fail
		var connected bool
		for i := 0; i < len(p.Addrs); i++ {
			err := <-resultChan
			if err == nil {
				connected = true
				if verbose {
					_, _ = fmt.Fprintln(os.Stderr, "Connected to sender")
				}
				break
			}
		}
		conCtxCancel()

		if !connected {
			_, _ = fmt.Fprintln(os.Stderr, "Failed to connect to sender on any address")
			os.Exit(1)
		}

		// wait for hole punch success or timeout
		stopSpinner := spinner("Establishing direct connection")
		hpCtx, hpCtxCancel := context.WithTimeout(context.Background(), holePunchTimeout)
		select {
		case <-holePunchedCh:
			stopSpinner()
			_, _ = fmt.Fprintln(os.Stderr, "Direct connection established")
		case <-hpCtx.Done():
			stopSpinner()
			_, _ = fmt.Fprintln(os.Stderr, "Timed out waiting for direct connection, using relay")
		}
		hpCtxCancel()
		node.Network().StopNotify(notifiee)

		// establish stream
		stopSpinner = spinner("Establishing stream")
		ctx, ctxCancel := context.WithTimeout(context.Background(), 30*time.Second)
		stream, err := node.NewStream(ctx, p.ID, transfer.ProtocolID)
		ctxCancel()
		stopSpinner()

		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Failed to open stream to peer. You may need to find a relay without limits.")
			os.Exit(1)
		}

		// receive data
		if err := transfer.ReceiveFile(stream); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Transfer failed: %s\n", err)
			os.Exit(1)
		}
	},
}

func spinner(description string) func() {
	stop := make(chan struct{})
	done := make(chan struct{})

	s := progressbar.NewOptions(-1,
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionThrottle(100*time.Millisecond),
	)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				_ = s.Clear()
				_ = s.Close()
				_, _ = fmt.Fprintln(os.Stderr)
				close(done)
				return

			case <-ticker.C:
				_ = s.Add(1)
			}
		}
	}()

	return func() {
		close(stop)
		// wait until cleanup is done
		<-done
	}
}

func init() {
	rootCmd.AddCommand(receiveCmd)
	receiveCmd.Flags().BoolVarP(&receiveYesOverwrite, "yes", "y", false, "Automatically overwrite existing files without prompting")
	receiveCmd.Flags().DurationVarP(&holePunchTimeout, "dc-timeout", "t", time.Second*20, "Direct connection timeout")
}

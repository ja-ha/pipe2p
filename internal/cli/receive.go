package cli

import (
	"context"
	"fmt"
	"log"
	"os"
	"pipe2p/internal/transfer"
	"time"

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
		log.SetOutput(os.Stderr)

		transfer.Verbose = verbose
		transfer.AutoAcceptOverwrite = receiveYesOverwrite

		peerCode := args[0]
		p, err := decodePeerAddrinfo(peerCode)
		if err != nil {
			log.Fatalln("Failed to decode peer code: ", err)
		}

		if len(p.Addrs) == 0 {
			log.Fatal("No multiaddress in peer record")
		}

		node, err := NewNode("")
		if err != nil {
			log.Fatalln("Failed to start node: ", err)
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
					log.Println("Trying connection via ", a)
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
					log.Println("Connected to sender")
				}
				break
			}
		}
		conCtxCancel()

		if !connected {
			log.Fatalln("Failed to connect to sender on any address")
		}

		// wait for hole punch success or timeout
		stopSpinner := spinner("Establishing direct connection")
		hpCtx, hpCtxCancel := context.WithTimeout(context.Background(), holePunchTimeout)
		select {
		case <-holePunchedCh:
			stopSpinner()
			log.Println("Direct connection established")
		case <-hpCtx.Done():
			stopSpinner()
			log.Println("Timed out waiting for direct connection, using relay")
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
			log.Fatalln("Failed to open stream to peer. You may need to find a relay without limits.")
		}

		// receive data
		if err := transfer.ReceiveFile(stream); err != nil {
			log.Fatalln("Transfer failed: ", err)
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
	receiveCmd.Flags().DurationVarP(&holePunchTimeout, "dc-timeout", "t", time.Second*10, "Direct connection timeout")
}

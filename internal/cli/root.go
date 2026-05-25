package cli

import (
	"os"

	"github.com/spf13/cobra"
)

var customRelay string
var defaultRelay = ""

var verbose bool

var version = "development"

var rootCmd = &cobra.Command{
	Use:     "pipe2p",
	Short:   "A P2P transfer utility",
	Long:    "pipe2p is a secure, peer-to-peer data transfer tool built on libp2p.",
	Version: version,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&customRelay, "relay", defaultRelay, "Relay node multiaddress to use instead of defaults")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose/debug logging output")
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

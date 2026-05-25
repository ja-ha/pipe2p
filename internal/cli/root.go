package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var customRelay string
var defaultRelay = ""

var verbose bool

var version = "development"
var commit = "none"
var date = "unknown"

var rootCmd = &cobra.Command{
	Use:     "pipe2p",
	Short:   "A P2P transfer utility",
	Long:    "pipe2p is a secure, peer-to-peer data transfer tool built on libp2p.",
	Version: buildVersion(),
}

func init() {
	rootCmd.PersistentFlags().StringVar(&customRelay, "relay", defaultRelay, "Relay node multiaddress to use instead of defaults")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose/debug logging output")
}

func buildVersion() string {
	return fmt.Sprintf("%s (commit: %s, built: %s)", version, commit, date)
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

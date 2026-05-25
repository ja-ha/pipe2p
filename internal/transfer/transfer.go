package transfer

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"
)

var Verbose bool
var AutoAcceptOverwrite bool

const ProtocolID = "/pipe2p/transfer/1.0.0"

type Metadata struct {
	Name string `json:"name"`
	Size int64  `json:"size"` // -1 indicates an unknown stream size (stdin)
}

func HandleIncomingStream(stream network.Stream, source io.Reader, name string, size int64, done chan struct{}, connected chan struct{}) {
	defer stream.Close()
	defer func() { done <- struct{}{} }()

	connected <- struct{}{}

	// user interrupt handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sigDone := make(chan struct{})
	defer close(sigDone)

	go func() {
		select {
		case <-sigChan:
			log.Println("\nUser interrupt. Canceling transfer...")
			_ = stream.ResetWithError(network.StreamShutdown)
		case <-sigDone:
		}
	}()

	if Verbose {
		log.Println("Receiver connected")
	}

	meta := Metadata{Name: name, Size: size}
	metaBytes, _ := json.Marshal(meta)
	metaLen := int64(len(metaBytes))

	if err := binary.Write(stream, binary.BigEndian, metaLen); err != nil {
		log.Println("Error writing metadata length: ", err)
		return
	}

	if err := binary.Write(stream, binary.BigEndian, metaBytes); err != nil {
		log.Fatalf("Error writing metadata: %s", err)
		return
	}

	if Verbose {
		log.Println("Sent metadata")
	}

	bar := getBar(name != "", size, "Uploading")

	writer := io.MultiWriter(stream, bar)

	buf := make([]byte, 1024*1024) // 1 MB
	_, err := io.CopyBuffer(writer, source, buf)
	if err != nil {
		_ = bar.Exit()
		_ = bar.Close()
		_, _ = fmt.Fprintln(os.Stderr)
		log.Println("Transfer interrupted: ", err)
		return
	}

	// Wait for the receiver to acknowledge they got the whole stream
	if err = stream.CloseWrite(); err != nil {
		return
	}
	ack := make([]byte, 1)
	if _, err = io.ReadFull(stream, ack); err != nil {
		log.Println("Receiver did not send success signal: ", err)
		return
	}

	_ = bar.Clear()
	_ = bar.Close()

	_, _ = fmt.Fprintln(os.Stderr)
	log.Println("Transfer complete!")
}

func ReceiveFile(stream network.Stream) error {
	defer stream.Close()

	// user interrupt handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sigDone := make(chan struct{})
	defer close(sigDone)

	go func() {
		select {
		case <-sigChan:
			log.Println("\nUser interrupt. Canceling transfer...")
			_ = stream.ResetWithError(network.StreamShutdown)
		case <-sigDone:
		}
	}()

	// read metadata length & JSON
	var metaLen int64
	if err := binary.Read(stream, binary.BigEndian, &metaLen); err != nil {
		log.Println("Error reading metadata length:", err)
	}

	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(stream, metaBytes); err != nil {
		return err
	}

	var meta Metadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return err
	}

	var out io.Writer

	// determine output destination
	if meta.Name == "" {
		// no filename means stdout
		out = os.Stdout

		// prompt for confirmation if stdout is attached to a terminal screen
		if term.IsTerminal(int(os.Stdout.Fd())) {
			if _, err := fmt.Fprintln(os.Stderr, "\n[WARN] Incoming stream has no filename and will print to the terminal."); err != nil {
				return err
			}
			if _, err := fmt.Fprint(os.Stderr, "Do you want to continue? (y/n): "); err != nil {
				return err
			}

			reader := bufio.NewReader(os.Stdin)
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(strings.ToLower(response))

			if response != "y" && response != "yes" {
				_ = stream.ResetWithError(network.StreamShutdown)
				return fmt.Errorf("transfer aborted by user")
			}
		}
	} else {
		savePath := filepath.Join(".", meta.Name)

		if _, err := os.Stat(savePath); err == nil {
			if !AutoAcceptOverwrite {
				log.Printf("File '%s' already exists.", meta.Name)
				if _, err = fmt.Fprint(os.Stderr, "Do you want to overwrite it? (y/n): "); err != nil {
					return err
				}

				reader := bufio.NewReader(os.Stdin)
				response, _ := reader.ReadString('\n')
				response = strings.TrimSpace(strings.ToLower(response))

				if response != "y" && response != "yes" {
					_ = stream.ResetWithError(network.StreamShutdown)
					return fmt.Errorf("transfer aborted: refused to overwrite existing file")
				}
			}
		}

		file, err := os.Create(savePath)
		if err != nil {
			return err
		}
		defer file.Close()

		out = file

		log.Println("Receiving file: ", meta.Name)
	}

	bar := getBar(meta.Name != "", meta.Size, "Downloading")
	defer bar.Close()

	buf := make([]byte, 1024*1024) // 1 MB
	writer := io.MultiWriter(out, bar)
	_, err := io.CopyBuffer(writer, stream, buf)
	if err != nil {
		return fmt.Errorf("transfer interrupted: %v", err)
	}

	if err = bar.Finish(); err != nil {
		return err
	}

	_, _ = fmt.Fprintln(os.Stderr)
	log.Println("Transfer complete!")

	// Send success signal to sender
	if _, err = stream.Write([]byte{1}); err != nil {
		return err
	}

	return nil
}

func getBar(isFile bool, size int64, desc string) *progressbar.ProgressBar {
	opts := []progressbar.Option{
		progressbar.OptionSetDescription(desc),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(true),
		progressbar.OptionThrottle(time.Second / 60),
	}

	if isFile {
		opts = append(opts, progressbar.OptionShowCount())
	}

	return progressbar.NewOptions64(size, opts...)
}

package transfer

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/DataDog/zstd"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"
)

var Verbose bool
var AutoAcceptOverwrite bool
var Compress bool
var CompressLvl int

const ProtocolID = "/pipe2p/transfer/1.0.0"

type Metadata struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"` // -1 indicates an unknown stream size (stdin)
	Compress bool   `json:"compress"`
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
			_, _ = fmt.Fprintln(os.Stderr, "\nUser interrupt. Canceling transfer...")
			_ = stream.ResetWithError(network.StreamShutdown)
		case <-sigDone:
		}
	}()

	if Verbose {
		_, _ = fmt.Fprintln(os.Stderr, "Receiver connected")
	}

	meta := Metadata{Name: name, Size: size, Compress: Compress}
	metaBytes, _ := json.Marshal(meta)
	metaLen := int64(len(metaBytes))

	if err := binary.Write(stream, binary.BigEndian, metaLen); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error writing metadata length: %s\n", err)
		return
	}

	if err := binary.Write(stream, binary.BigEndian, metaBytes); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error writing metadata: %s\n", err)
		return
	}

	if Verbose {
		_, _ = fmt.Fprintln(os.Stderr, "Sent metadata")
	}

	bar := getBar(name != "", size, "Uploading")

	var writer io.Writer
	var zstdWriter *zstd.Writer
	if Compress {
		zstdWriter = zstd.NewWriterLevel(stream, CompressLvl)
		defer zstdWriter.Close()

		writer = io.MultiWriter(zstdWriter, bar)

		if Verbose {
			_, _ = fmt.Fprintf(os.Stderr, "Compressing data using zstd level %d\n", CompressLvl)
		}

	} else {
		writer = io.MultiWriter(stream, bar)
	}

	buf := make([]byte, 1024*1024) // 1 MB
	_, err := io.CopyBuffer(writer, source, buf)
	if err != nil {
		_ = bar.Exit()
		_ = bar.Close()
		_, _ = fmt.Fprintf(os.Stderr, "\nTransfer interrupted: %s\n", err)
		return
	}

	if Compress {
		if err = zstdWriter.Close(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Failed to close zstd stream: ", err)
			return
		}
	}

	if err = stream.CloseWrite(); err != nil {
		return
	}

	// Wait for the receiver to acknowledge they got the whole stream
	ack := make([]byte, 1)
	if _, err = io.ReadFull(stream, ack); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Receiver did not send success signal: %s\n", err)
		return
	}

	_ = bar.Clear()
	_ = bar.Close()

	_, _ = fmt.Fprintln(os.Stderr, "\nTransfer complete!")
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
			_, _ = fmt.Fprintln(os.Stderr, "\nUser interrupt. Canceling transfer...")
			_ = stream.ResetWithError(network.StreamShutdown)
		case <-sigDone:
		}
	}()

	// read metadata length & JSON
	var metaLen int64
	if err := binary.Read(stream, binary.BigEndian, &metaLen); err != nil {
		return fmt.Errorf("error reading metadata length: %s", err)
	}

	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(stream, metaBytes); err != nil {
		return fmt.Errorf("error reading metadata: %s", err)
	}

	var meta Metadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return fmt.Errorf("error parsing metadata: %s", err)
	}

	var out io.Writer

	// determine output destination
	if meta.Name == "" {
		// no filename means stdout
		out = os.Stdout

		// prompt for confirmation if stdout is attached to a terminal screen
		if term.IsTerminal(int(os.Stdout.Fd())) {
			if _, err := fmt.Fprint(os.Stderr, "\nWARNING: Incoming stream has no filename and will print to the terminal.\n"+
				"Do you want to continue? (y/n): "); err != nil {
				return err
			}

			reader := bufio.NewReader(os.Stdin)
			response, err := reader.ReadString('\n')
			if err != nil {
				return fmt.Errorf("could not read user response: %s", err)
			}
			response = strings.TrimSpace(strings.ToLower(response))

			if response != "y" && response != "yes" {
				err = stream.ResetWithError(network.StreamShutdown)
				return errors.Join(err, fmt.Errorf("transfer aborted by user"))
			}
		}
	} else {
		savePath := filepath.Join(".", meta.Name)

		if _, err := os.Stat(savePath); err == nil {
			if !AutoAcceptOverwrite {
				if _, err = fmt.Fprintf(os.Stderr, "File '%s' already exists.\nDo you want to overwrite it? (y/n): ", meta.Name); err != nil {
					return err
				}

				reader := bufio.NewReader(os.Stdin)
				response, err := reader.ReadString('\n')
				if err != nil {
					return fmt.Errorf("could not read user response: %s", err)
				}
				response = strings.TrimSpace(strings.ToLower(response))

				if response != "y" && response != "yes" {
					_ = stream.ResetWithError(network.StreamShutdown)
					return fmt.Errorf("transfer aborted: User declined to overwrite existing file")
				}
			}
		}

		file, err := os.Create(savePath)
		if err != nil {
			return fmt.Errorf("error creating file '%s': %s", savePath, err)
		}
		defer file.Close()

		out = file

		_, _ = fmt.Fprintf(os.Stderr, "Receiving file: %s\n", meta.Name)
	}

	bar := getBar(meta.Name != "", meta.Size, "Downloading")
	defer bar.Close()

	buf := make([]byte, 1024*1024) // 1 MB
	writer := io.MultiWriter(out, bar)

	var err error
	if meta.Compress {
		if Verbose {
			_, _ = fmt.Fprintln(os.Stderr, "Incoming data is compressed")
		}
		zstdReader := zstd.NewReader(stream)
		_, err = io.CopyBuffer(writer, zstdReader, buf)
		err = errors.Join(err, zstdReader.Close())
	} else {
		_, err = io.CopyBuffer(writer, stream, buf)
	}

	if err != nil {
		return fmt.Errorf("transfer interrupted: %v", err)
	}

	if err = bar.Finish(); err != nil {
		return err
	}

	_, _ = fmt.Fprintln(os.Stderr, "\nTransfer complete!")

	// Send success signal to sender
	if _, err = stream.Write([]byte{1}); err != nil {
		return fmt.Errorf("unable to signal success to sender: %v", err)
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

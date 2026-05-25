package transfer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type testStream struct {
	reader    *bytes.Reader
	writes    bytes.Buffer
	protocol  protocol.ID
	resetCode network.StreamErrorCode
}

func (s *testStream) Read(p []byte) (int, error)                 { return s.reader.Read(p) }
func (s *testStream) Write(p []byte) (int, error)                { return s.writes.Write(p) }
func (s *testStream) Close() error                               { return nil }
func (s *testStream) CloseWrite() error                          { return nil }
func (s *testStream) CloseRead() error                           { return nil }
func (s *testStream) Reset() error                               { return nil }
func (s *testStream) SetDeadline(time.Time) error                { return nil }
func (s *testStream) SetReadDeadline(time.Time) error            { return nil }
func (s *testStream) SetWriteDeadline(time.Time) error           { return nil }
func (s *testStream) ID() string                                 { return "test-stream" }
func (s *testStream) Protocol() protocol.ID                      { return s.protocol }
func (s *testStream) SetProtocol(id protocol.ID) error           { s.protocol = id; return nil }
func (s *testStream) Stat() network.Stats                        { return network.Stats{} }
func (s *testStream) Conn() network.Conn                         { return nil }
func (s *testStream) Scope() network.StreamScope                 { return nil }
func (s *testStream) ResetWithError(code network.StreamErrorCode) error { s.resetCode = code; return nil }

func TestReceiveFileWritesUncompressedFileAndAcks(t *testing.T) {
	data := []byte("hello from pipe2p")
	streamData := mustBuildStreamData(t, Metadata{
		Name:     "out.txt",
		Size:     int64(len(data)),
		Compress: false,
	}, data)

	stream := &testStream{reader: bytes.NewReader(streamData)}

	restore := chdirTemp(t)
	defer restore()

	AutoAcceptOverwrite = true
	err := ReceiveFile(stream)
	if err != nil {
		t.Fatalf("ReceiveFile returned error: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(".", "out.txt"))
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("output file mismatch: got %q, want %q", got, data)
	}
	if !bytes.Equal(stream.writes.Bytes(), []byte{1}) {
		t.Fatalf("missing success ack write: got %v", stream.writes.Bytes())
	}
}

func TestReceiveFileWritesCompressedFileAndAcks(t *testing.T) {
	data := []byte("compressed payload content")
	compressed := mustZstdCompress(t, data)

	streamData := mustBuildStreamData(t, Metadata{
		Name:     "compressed.txt",
		Size:     int64(len(data)),
		Compress: true,
	}, compressed)

	stream := &testStream{reader: bytes.NewReader(streamData)}

	restore := chdirTemp(t)
	defer restore()

	AutoAcceptOverwrite = true
	err := ReceiveFile(stream)
	if err != nil {
		t.Fatalf("ReceiveFile returned error: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(".", "compressed.txt"))
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("output file mismatch: got %q, want %q", got, data)
	}
	if !bytes.Equal(stream.writes.Bytes(), []byte{1}) {
		t.Fatalf("missing success ack write: got %v", stream.writes.Bytes())
	}
}

func TestReceiveFileRejectsInvalidMetadataJSON(t *testing.T) {
	var raw bytes.Buffer
	metaBytes := []byte("{not valid json")
	if err := binary.Write(&raw, binary.BigEndian, int64(len(metaBytes))); err != nil {
		t.Fatalf("write metadata length: %v", err)
	}
	if _, err := raw.Write(metaBytes); err != nil {
		t.Fatalf("write metadata bytes: %v", err)
	}

	stream := &testStream{reader: bytes.NewReader(raw.Bytes())}
	if err := ReceiveFile(stream); err == nil {
		t.Fatal("expected ReceiveFile to fail for invalid metadata JSON")
	}
}

func mustBuildStreamData(t *testing.T, meta Metadata, payload []byte) []byte {
	t.Helper()

	var raw bytes.Buffer
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	if err = binary.Write(&raw, binary.BigEndian, int64(len(metaBytes))); err != nil {
		t.Fatalf("write metadata length: %v", err)
	}
	if _, err = raw.Write(metaBytes); err != nil {
		t.Fatalf("write metadata bytes: %v", err)
	}
	if _, err = raw.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	return raw.Bytes()
}

func mustZstdCompress(t *testing.T, data []byte) []byte {
	t.Helper()

	var b bytes.Buffer
	w, err := zstd.NewWriter(&b)
	if err != nil {
		t.Fatalf("create zstd writer: %v", err)
	}
	if _, err = w.Write(data); err != nil {
		t.Fatalf("write zstd data: %v", err)
	}
	if err = w.Close(); err != nil {
		t.Fatalf("close zstd writer: %v", err)
	}
	return b.Bytes()
}

func chdirTemp(t *testing.T) func() {
	t.Helper()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	dir := t.TempDir()
	if err = os.Chdir(dir); err != nil {
		t.Fatalf("chdir temp: %v", err)
	}

	return func() {
		if err := os.Chdir(cwd); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	}
}

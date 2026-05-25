package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestLoadOrGenerateKeyLoadsExistingKey(t *testing.T) {
	t.Parallel()

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	marshaled, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}

	path := filepath.Join(t.TempDir(), "relay.key")
	if err = os.WriteFile(path, marshaled, 0600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	loaded, err := loadOrGenerateKey(path)
	if err != nil {
		t.Fatalf("loadOrGenerateKey: %v", err)
	}

	loadedBytes, err := crypto.MarshalPrivateKey(loaded)
	if err != nil {
		t.Fatalf("marshal loaded key: %v", err)
	}
	if string(loadedBytes) != string(marshaled) {
		t.Fatal("loaded key differs from stored key")
	}
}

func TestLoadOrGenerateKeyGeneratesAndPersistsKey(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "relay.key")
	key, err := loadOrGenerateKey(path)
	if err != nil {
		t.Fatalf("loadOrGenerateKey: %v", err)
	}
	if key == nil {
		t.Fatal("expected generated key, got nil")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("key file permissions are too permissive: %o", info.Mode().Perm())
	}
}

func TestLoadOrGenerateKeyFailsOnUnreadableExistingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "relay.key")
	if err := os.WriteFile(path, []byte("not-a-valid-private-key"), 0600); err != nil {
		t.Fatalf("write invalid key file: %v", err)
	}

	if _, err := loadOrGenerateKey(path); err == nil {
		t.Fatal("expected loadOrGenerateKey to fail for invalid key file")
	}
}

func TestLoadOrGenerateKeyFailsWhenPathIsDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if _, err := loadOrGenerateKey(dir); err == nil {
		t.Fatal("expected loadOrGenerateKey to fail when path is a directory")
	}
}

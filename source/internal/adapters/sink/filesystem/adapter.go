// Package filesystem adapts burnside-go's storage.ReaderWriter as a ports.Sink
// for local filesystem output.
package filesystem

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/burnside-project/burnside-go/manifest"
	"github.com/burnside-project/burnside-go/storage"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// Adapter wraps burnside-go's storage backend as a ports.Sink.
type Adapter struct {
	store storage.ReaderWriter
}

// Verify interface compliance at compile time.
var _ ports.Sink = (*Adapter)(nil)

// New creates a filesystem sink from a storage path (e.g., "/var/lib/pg-cdc/output/").
func New(path string) (*Adapter, error) {
	store, err := storage.ParseStorageURL(path)
	if err != nil {
		return nil, fmt.Errorf("parse storage URL %q: %w", path, err)
	}
	return &Adapter{store: store}, nil
}

func (a *Adapter) WriteFile(ctx context.Context, remotePath string, r io.Reader) error {
	return a.store.WriteFile(ctx, remotePath, r)
}

func (a *Adapter) ReadFile(ctx context.Context, path string) (io.ReadCloser, error) {
	return a.store.ReadFile(ctx, path)
}

func (a *Adapter) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	return a.store.ListFiles(ctx, prefix)
}

// Exists reports whether the file exists. It stats the path directly when the
// underlying store exposes its base directory, so the answer is exact for a
// manifest-named file and costs one syscall.
func (a *Adapter) Exists(_ context.Context, path string) (bool, error) {
	if fsStore, ok := a.store.(*storage.Filesystem); ok {
		_, err := os.Stat(filepath.Join(fsStore.BasePath(), filepath.FromSlash(path)))
		if err == nil {
			return true, nil
		}
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	// Fallback for any other ReaderWriter: open and discard the handle.
	f, err := a.store.ReadFile(context.Background(), path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	_ = f.Close()
	return true, nil
}

func (a *Adapter) DeleteFile(ctx context.Context, path string) error {
	return a.store.DeleteFile(ctx, path)
}

func (a *Adapter) ReadManifest(ctx context.Context) (*manifest.Manifest, error) {
	return a.store.ReadManifest(ctx)
}

func (a *Adapter) WriteManifest(ctx context.Context, m *manifest.Manifest) error {
	return a.store.WriteManifest(ctx, m)
}

func (a *Adapter) Backend() string {
	return "filesystem"
}
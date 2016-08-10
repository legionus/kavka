package storage

import (
	"errors"
	"io"

	"github.com/legionus/kavka/pkg/digest"
)

const (
	AppStorageDriverContextVar = "app.storage"
)

type StorageDriverParameters map[string]string

type Descriptor struct {
	// Digest uniquely identifies the content. A byte stream can be verified
	// against against this digest.
	Digest digest.Digest `json:"digest,omitempty"`

	// Size in bytes of content.
	Size int64 `json:"size,omitempty"`
}

type Blob []byte

var (
	// ErrBlobExists returned when blob already exists
	ErrBlobExists = errors.New("blob exists")

	// ErrBlobUnknown when blob is not found.
	ErrBlobUnknown = errors.New("unknown blob")
)

type StorageDriver interface {
	Name() string
	Has(digest.Digest) (bool, error)
	Stat(digest.Digest) (Descriptor, error)
	Read(digest.Digest) (Blob, error)
	Reader(digest.Digest) (io.ReadCloser, error)
	Write(Blob) (digest.Digest, error)
	Delete(digest.Digest) error
	Close() error
}

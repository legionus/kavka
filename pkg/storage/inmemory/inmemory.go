package inmemory

import (
	"bytes"
	"io"
	"sync"

	"github.com/legionus/kavka/pkg/digest"
	"github.com/legionus/kavka/pkg/storage"
	"github.com/legionus/kavka/pkg/storage/factory"
)

const driverName = "inmemory"

func init() {
	factory.Register(driverName, &inMemoryDriverFactory{})
}

type inMemoryDriverFactory struct{}

func (f *inMemoryDriverFactory) Create(parameters storage.StorageDriverParameters) (storage.StorageDriver, error) {
	return &driver{
		data: make(map[digest.Digest]storage.Blob),
	}, nil
}

type driver struct {
	mutex sync.RWMutex

	data map[digest.Digest]storage.Blob
}

func (d *driver) Name() string {
	return driverName
}

func (d *driver) Close() error {
	return nil
}

func (d *driver) Has(dgst digest.Digest) (bool, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	_, ok := d.data[dgst]
	return ok, nil
}

func (d *driver) Stat(dgst digest.Digest) (storage.Descriptor, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	v, ok := d.data[dgst]
	if !ok {
		return storage.Descriptor{}, storage.ErrBlobUnknown
	}

	return storage.Descriptor{
		Digest: dgst,
		Size:   int64(len(v)),
	}, nil
}

func (d *driver) Read(dgst digest.Digest) (storage.Blob, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if v, ok := d.data[dgst]; ok {
		return v, nil
	}
	return nil, storage.ErrBlobUnknown
}

type blobReadCloser struct {
	io.Reader
}

func (br *blobReadCloser) Close() error {
	return nil
}

func (d *driver) Reader(dgst digest.Digest) (io.ReadCloser, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if v, ok := d.data[dgst]; ok {
		return &blobReadCloser{bytes.NewReader(v)}, nil
	}
	return nil, storage.ErrBlobUnknown
}

func (d *driver) Write(blob storage.Blob) (digest.Digest, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	dgst := digest.FromBytes(blob)

	if _, ok := d.data[dgst]; ok {
		return "", storage.ErrBlobExists
	}

	d.data[dgst] = blob
	return dgst, nil
}

func (d *driver) Delete(dgst digest.Digest) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	delete(d.data, dgst)
	return nil
}

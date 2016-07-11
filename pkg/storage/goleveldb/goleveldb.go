package goleveldb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/legionus/kavka/pkg/digest"
	"github.com/legionus/kavka/pkg/storage"
	"github.com/legionus/kavka/pkg/storage/factory"
)

const driverName = "goleveldb"

func init() {
	factory.Register(driverName, &goLeveldbDriverFactory{})
}

type goLeveldbDriverFactory struct{}

func (f *goLeveldbDriverFactory) Create(parameters storage.StorageDriverParameters) (storage.StorageDriver, error) {
	rootdir, ok := parameters["rootdir"]
	if !ok {
		return nil, fmt.Errorf("rootdir not specified")
	}

	db, err := leveldb.OpenFile(rootdir, nil)
	if err != nil {
		return nil, err
	}

	return &driver{
		db: db,
	}, nil
}

type driver struct {
	db *leveldb.DB
}

func (d *driver) Name() string {
	return driverName
}

func (d *driver) Close() error {
	return d.db.Close()
}

func (d *driver) Has(dgst digest.Digest) (bool, error) {
	return d.db.Has([]byte(dgst), nil)
}

func (d *driver) Read(dgst digest.Digest) (storage.Blob, error) {
	if has, err := d.Has(dgst); err != nil {
		return nil, err
	} else if !has {
		return nil, storage.ErrBlobUnknown
	}

	return d.db.Get([]byte(dgst), nil)
}

type blobReadCloser struct {
	io.Reader
}

func (br *blobReadCloser) Close() error {
	return nil
}

func (d *driver) Reader(dgst digest.Digest) (io.ReadCloser, error) {
	v, err := d.Read(dgst)
	if err != nil {
		return nil, err
	}
	return &blobReadCloser{bytes.NewReader(v)}, nil
}

func (d *driver) Write(blob storage.Blob) (digest.Digest, error) {
	dgst := digest.FromBytes(blob)
	err := d.db.Put([]byte(dgst), blob, &opt.WriteOptions{Sync: true})

	return dgst, err
}

func (d *driver) Delete(dgst digest.Digest) error {
	return d.db.Delete([]byte(dgst), nil)
}

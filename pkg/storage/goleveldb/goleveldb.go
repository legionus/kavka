package goleveldb

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/legionus/kavka/pkg/digest"
	"github.com/legionus/kavka/pkg/storage"
	"github.com/legionus/kavka/pkg/storage/factory"
	"github.com/legionus/kavka/pkg/util"
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

func (d *driver) Stat(dgst digest.Digest) (storage.Descriptor, error) {
	desc := storage.Descriptor{
		Digest: dgst,
	}

	if has, err := d.Has(dgst); err != nil {
		return desc, err
	} else if !has {
		return desc, storage.ErrBlobUnknown
	}

	v, err := d.db.Get([]byte("size:"+dgst.String()), nil)
	if err != nil {
		return desc, err
	}

	desc.Size = util.ToInt64(string(v))
	return desc, nil
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

	batch := &leveldb.Batch{}
	batch.Put([]byte(dgst), blob)
	batch.Put([]byte("size:"+dgst.String()), []byte(fmt.Sprintf("%d", len(blob))))

	transaction, err := d.db.OpenTransaction()
	if err != nil {
		return dgst, err
	}

	if err := transaction.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		transaction.Discard()
		return dgst, err
	}

	return dgst, transaction.Commit()
}

func (d *driver) Delete(dgst digest.Digest) error {
	batch := &leveldb.Batch{}
	batch.Delete([]byte(dgst))
	batch.Delete([]byte("size:" + dgst.String()))

	transaction, err := d.db.OpenTransaction()
	if err != nil {
		return err
	}

	if err := transaction.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		transaction.Discard()
		return err
	}

	return transaction.Commit()
}

func (d *driver) Iterate(handler func(k storage.Key, v storage.Blob) (bool, error)) error {
	s, err := d.db.GetSnapshot()
	if err != nil {
		return err
	}
	defer s.Release()

	iter := d.db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		key := storage.Key(iter.Key())

		if strings.HasPrefix(string(key), "size:") {
			continue
		}

		finish, err := handler(key, iter.Value())

		if err != nil {
			return err
		}

		if finish {
			break
		}
	}

	return iter.Error()
}

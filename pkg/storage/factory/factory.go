package factory

import (
	"fmt"

	"github.com/legionus/kavka/pkg/storage"
)

var driverFactories = make(map[string]StorageDriverFactory)

type StorageDriverFactory interface {
	Create(parameters storage.StorageDriverParameters) (storage.StorageDriver, error)
}

func Register(name string, factory StorageDriverFactory) {
	if factory == nil {
		panic("Must not provide nil StorageDriverFactory")
	}
	_, registered := driverFactories[name]
	if registered {
		panic(fmt.Sprintf("StorageDriverFactory named %s already registered", name))
	}

	driverFactories[name] = factory
}

func Create(name string, parameters storage.StorageDriverParameters) (storage.StorageDriver, error) {
	driverFactory, ok := driverFactories[name]
	if !ok {
		return nil, InvalidStorageDriverError{name}
	}
	return driverFactory.Create(parameters)
}

// InvalidStorageDriverError records an attempt to construct an unregistered storage driver
type InvalidStorageDriverError struct {
	Name string
}

func (err InvalidStorageDriverError) Error() string {
	return fmt.Sprintf("StorageDriver not registered: %s", err.Name)
}

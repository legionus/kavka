package observer

import (
	"sync"
)

type EtcdFilter struct {
	sync.RWMutex

	observer  *EtcdObserver
	filter    EtcdObserveHandler
	handlerID EtcdObserveKey
}

func NewEtcdFilter(observer *EtcdObserver, handler EtcdObserveHandler) (*EtcdFilter, error) {
	return &EtcdFilter{
		observer: observer,
		filter:   handler,
	}, nil
}

func (f *EtcdFilter) Start() *EtcdFilter {
	f.Lock()
	defer f.Unlock()

	f.handlerID = f.observer.RegisterHandler(f.filter)
	return f
}

func (f *EtcdFilter) Stop() {
	f.Lock()
	defer f.Unlock()

	if f.handlerID == "" {
		return
	}
	f.observer.UnregisterHandler(f.handlerID)
	f.handlerID = ""
}

package etcd

import (
	"crypto/tls"
	"fmt"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
)

type EtcdClient struct {
	*v3.Client
}

func useTLS(servingInfo config.ServingInfo) bool {
	return len(servingInfo.SecurityConfig.CertFile) > 0
}

// NewEtcdClient creates an etcd client based on the provided config.
func NewEtcdClient(cfg *config.Config) (*EtcdClient, error) {
	var (
		err           error
		clientTLSInfo transport.TLSInfo
		ctlscfg       *tls.Config
	)

	if useTLS(cfg.Etcd.Client) {
		clientTLSInfo.CertFile = cfg.Etcd.Client.CertFile
		clientTLSInfo.KeyFile = cfg.Etcd.Client.KeyFile
		clientTLSInfo.ClientCertAuth = cfg.Etcd.Client.CertAuth
		clientTLSInfo.TrustedCAFile = cfg.Etcd.Client.TrustedCAFile
	}

	if !clientTLSInfo.Empty() {
		ctlscfg, err = clientTLSInfo.ServerConfig()
		if err != nil {
			return nil, err
		}
	}

	etcdClient, err := v3.New(v3.Config{
		Endpoints:   cfg.Etcd.Client.URLs,
		TLS:         ctlscfg,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &EtcdClient{etcdClient}, nil
}

func (c *EtcdClient) GetClient() *v3.Client {
	return c.Client
}

// TestEtcdClient verifies a client is functional.  It will attempt to
// connect to the etcd server and block until the server responds at least once, or return an
// error if the server never responded.
func (c *EtcdClient) TestEtcdClient() error {
	ctx := context.Background()

	for i := 0; ; i++ {
		_, err := c.Get(ctx, "/")
		if err == nil {
			break
		}
		if i > 100 {
			return fmt.Errorf("could not reach etcd: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

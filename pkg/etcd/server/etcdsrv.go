package server

import (
	"net/url"

	"github.com/Sirupsen/logrus"

	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/pkg/transport"

	"github.com/legionus/kavka/pkg/config"
)

const (
	// the owner can make/remove files inside the directory
	defaultName = "kavka.local"
)

func copySecurityDetails(tls *transport.TLSInfo, ysc *config.SecurityConfig) {
	tls.CertFile = ysc.CertFile
	tls.KeyFile = ysc.KeyFile
	tls.ClientCertAuth = ysc.CertAuth
	tls.TrustedCAFile = ysc.TrustedCAFile
}

func setIfNotEmpty(orig string, v *string) string {
	if v != nil {
		return *v
	}
	return orig
}

func RunEtcd(etcdConfig *config.Etcd) {
	cfg := embed.NewConfig()

	cfg.Dir = etcdConfig.StorageDir

	cfg.Name = setIfNotEmpty(cfg.Name, etcdConfig.Name)
	cfg.InitialCluster = setIfNotEmpty(cfg.InitialCluster, etcdConfig.InitialCluster)
	cfg.ClusterState = setIfNotEmpty(cfg.ClusterState, etcdConfig.ClusterState)
	cfg.DNSCluster = setIfNotEmpty(cfg.DNSCluster, etcdConfig.Discovery.DNSCluster)
	cfg.Dproxy = setIfNotEmpty(cfg.Dproxy, etcdConfig.Discovery.Dproxy)
	cfg.Durl = setIfNotEmpty(cfg.Durl, etcdConfig.Discovery.Durl)

	if len(etcdConfig.Client.URLs) != 0 {
		cfg.LCUrls = []url.URL{}
		for _, addr := range etcdConfig.Client.URLs {
			addrURL, err := url.Parse(addr)
			if err != nil {
				logrus.Fatalf("unexpected error setting up client urls: %v", err)
			}
			cfg.LCUrls = append(cfg.LCUrls, *addrURL)
		}
	}

	if len(etcdConfig.Peer.URLs) != 0 {
		cfg.LPUrls = []url.URL{}
		for _, addr := range etcdConfig.Peer.URLs {
			addrURL, err := url.Parse(addr)
			if err != nil {
				logrus.Fatalf("unexpected error setting up peer urls: %v", err)
			}
			cfg.LPUrls = append(cfg.LPUrls, *addrURL)
		}
	}

	if len(etcdConfig.AdvertiseClient.URLs) != 0 {
		cfg.ACUrls = []url.URL{}
		for _, addr := range etcdConfig.AdvertiseClient.URLs {
			addrURL, err := url.Parse(addr)
			if err != nil {
				logrus.Fatalf("unexpected error setting up advertise client urls: %v", err)
			}
			cfg.ACUrls = append(cfg.ACUrls, *addrURL)
		}
	}

	if len(etcdConfig.AdvertisePeer.URLs) != 0 {
		cfg.APUrls = []url.URL{}
		for _, addr := range etcdConfig.AdvertisePeer.URLs {
			addrURL, err := url.Parse(addr)
			if err != nil {
				logrus.Fatalf("unexpected error setting up advertise peer urls: %v", err)
			}
			cfg.APUrls = append(cfg.APUrls, *addrURL)
		}
	}

	copySecurityDetails(&cfg.ClientTLSInfo, &etcdConfig.Client.SecurityConfig)
	copySecurityDetails(&cfg.PeerTLSInfo, &etcdConfig.Peer.SecurityConfig)

	cfg.ClientAutoTLS = etcdConfig.Client.AutoTLS
	cfg.PeerAutoTLS = etcdConfig.Peer.AutoTLS

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		logrus.Fatal(err)
	}
	go func() {
		logrus.Infof("Started etcd at %v", etcdConfig.Client.URLs)
		logrus.Fatal(<-e.Err())
	}()
}

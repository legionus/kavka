package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/altlinux/logfile-go"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	etcdclient "github.com/legionus/kavka/pkg/etcd"
	etcdobserver "github.com/legionus/kavka/pkg/etcd/observer"
	etcdserver "github.com/legionus/kavka/pkg/etcd/server"
	"github.com/legionus/kavka/pkg/metadata"
	"github.com/legionus/kavka/pkg/storage"
	"github.com/legionus/kavka/pkg/storage/factory"
	"github.com/legionus/kavka/pkg/syncer"
	"github.com/legionus/kavka/pkg/webapi"
	"github.com/legionus/kavka/pkg/webapi/handlers"
	"github.com/legionus/kavka/pkg/webapi/middleware/mlog"

	_ "github.com/legionus/kavka/pkg/storage/goleveldb"
	_ "github.com/legionus/kavka/pkg/storage/inmemory"
)

var (
	configFile = flag.String("config", "", "Path to configuration file")
)

func registerEtcd(cfg *config.Config, ctx context.Context) context.Context {
	if !cfg.Etcd.NoServer {
		log.Info("Register Etcd")
		etcdserver.RunEtcd(&cfg.Etcd)
	}

	client, err := etcdclient.NewEtcdClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	err = client.TestEtcdClient()
	if err != nil {
		log.Fatal(err)
	}
	log.Info("Etcd ready")

	nodesColl, err := metadata.NewNodesCollection(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	_, err = nodesColl.Create(
		&metadata.ClusterEtcdKey{
			Group: cfg.Global.Group,
			Node:  cfg.Global.Hostname,
		},
		fmt.Sprintf("%s", time.Now()),
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Info("Run etcd observer")
	etcdObserver, err := etcdobserver.NewEtcdObserver(cfg)
	if err != nil {
		log.Fatal(err)
	}
	etcdObserver.RunEtcdObserver(metadata.BlobsEtcd)

	return context.WithValue(ctx, metadata.BlobsObserverContextVar, etcdObserver)
}

func main() {
	flag.Parse()

	cfgFile := "./config.yaml"

	if v := os.Getenv("KAVKA_CONFIG"); v != "" {
		cfgFile = v
	}
	if *configFile != "" {
		cfgFile = *configFile
	}
	if cfgFile == "" {
		log.Fatal("Config file not found")
	}

	ctx := context.Background()

	cfg, err := config.NewConfig(cfgFile)
	if err != nil {
		log.Fatal(err)
	}
	ctx = context.WithValue(ctx, config.AppConfigContextVar, cfg)

	log.Infof("%#v", cfg.Etcd)

	log.SetLevel(cfg.Logging.Level.Level)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:    cfg.Logging.FullTimestamp,
		DisableTimestamp: cfg.Logging.DisableTimestamp,
		DisableColors:    cfg.Logging.DisableColors,
		DisableSorting:   cfg.Logging.DisableSorting,
	})

	if cfg.Global.Logfile != "stderr" && cfg.Global.Logfile != "/dev/stderr" {
		logFile, err := logfile.OpenLogfile(cfg.Global.Logfile)
		if err != nil {
			log.Fatal("Unable to open log: ", err.Error())
		}
		defer logFile.Close()
		log.SetOutput(logFile)
	}

	ctx = registerEtcd(cfg, ctx)

	log.Infof("Register backend driver: %s", cfg.Storage.Driver.Name())
	storageDriver, err := factory.Create(cfg.Storage.Driver.Name(), cfg.Storage.Driver.Parameters())
	if err != nil {
		log.Fatal(err)
	}
	defer storageDriver.Close()

	ctx = context.WithValue(ctx, storage.AppStorageDriverContextVar, storageDriver)
	ctx = context.WithValue(ctx, webapi.HTTPEndpointsContextVar, handlers.Endpoints)

	log.Info("Run blob syncer")
	syncer.RunSyncer(ctx)

	log.Info("Setup http interface")
	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		reqCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		mlog.Handler(handlers.Handler)(reqCtx, webapi.NewResponseWriter(w), req)
	})

	log.Info("Ready")
	log.Fatal(http.ListenAndServe(cfg.Global.Address, nil))
}

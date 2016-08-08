package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/metadata"
)

var (
	configFile = flag.String("config", "", "Path to configuration file")
	topic      = flag.String("topic", "", "topic name")
	partitions = flag.Int64("partitions", 1, "number of partitions")
)

func MakeKey(coll metadata.EtcdCollection, key metadata.EtcdKey, value string) error {
	if _, err := coll.Get(key); err != nil {
		if err != metadata.ErrKeyNotFound {
			return err
		}
		return coll.Put(key, value)
	}
	return nil
}

func main() {
	flag.Parse()

	if *topic == "" {
		log.Fatal("topic name required")
	}

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

	cfg, err := config.NewConfig(cfgFile)
	if err != nil {
		log.Fatal(err)
	}

	coll, err := metadata.NewTopicsCollection(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}

	val := fmt.Sprintf("%s", time.Now())
	key := &metadata.TopicEtcdKey{
		Topic:     *topic,
		Partition: 0,
	}

	for i := int64(0); i < *partitions; i++ {
		key.Partition = i

		fmt.Printf("%s %d\n", key.Topic, i)

		if err := MakeKey(coll, key, val); err != nil {
			log.Fatal(err)
		}
	}
}

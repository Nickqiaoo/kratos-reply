package dao

import (
	kafka "github.com/Shopify/sarama"
	"github.com/go-kratos/kratos/pkg/conf/paladin"
)

type KafkaConfig struct {
	Topic   string
	Brokers []string
}

func NewKafka() (k kafka.SyncProducer, cf func(), err error) {
	var (
		cfg KafkaConfig
		ct  paladin.Map
	)

	if err = ct.Get("kafka.toml").Unmarshal(&ct); err != nil {
		return
	}
	if err = ct.Get("Client").UnmarshalTOML(&cfg); err != nil {
		return
	}
	kc := kafka.NewConfig()
	kc.Producer.RequiredAcks = kafka.WaitForAll // Wait for all in-sync replicas to ack the message
	kc.Producer.Retry.Max = 10                  // Retry up to 10 times to produce the message
	kc.Producer.Return.Successes = true
	if k, err = kafka.NewSyncProducer(cfg.Brokers, kc); err != nil {
		return
	}
	cf = func() {}
	return
}

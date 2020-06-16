package dao

import (
	"context"
	"encoding/json"
	kafka "github.com/Shopify/sarama"
	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/go-kratos/kratos/pkg/log"
	"kratos-reply/internal/model"
	"strconv"
)

type KafkaConfig struct {
	Topic   string
	Brokers []string
}

type Kafka struct {
	Topic string
	kafka.SyncProducer
}

func NewKafka() (k *Kafka, cf func(), err error) {
	var (
		cfg      KafkaConfig
		ct       paladin.Map
		kafkaPro kafka.SyncProducer
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
	if kafkaPro, err = kafka.NewSyncProducer(cfg.Brokers, kc); err != nil {
		return
	}
	k = &Kafka{
		Topic:        cfg.Topic,
		SyncProducer: kafkaPro,
	}
	cf = func() {}
	return
}

func (d *dao) AddReply(c context.Context, oid int64, rp *model.Reply) {
	rpJson, err := json.Marshal(rp)
	if err != nil {
		log.Error("json marshal error(%v)", err)
		return
	}
	m := &kafka.ProducerMessage{
		Key:     kafka.StringEncoder(strconv.FormatInt(oid, 10)),
		Topic:   d.kafkaPub.Topic,
		Headers: []kafka.RecordHeader{{[]byte("action"), []byte("add")}},
		Value:   kafka.ByteEncoder(rpJson),
	}
	if _, _, err := d.kafkaPub.SendMessage(m); err != nil {
		log.Error("PushMsg.send(push pushMsg:%v) error(%v)", m, err)
	}
	return
}

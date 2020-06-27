package dao

import (
	"context"
	"encoding/json"
	kafka "github.com/Shopify/sarama"
	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/go-kratos/kratos/pkg/log"
	"github.com/pkg/errors"
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

type kafkadata struct {
	Op      string `json:"op,omitempty"`
	Mid     int64  `json:"mid,omitempty"`
	Adid    int64  `json:"adid,omitempty"`
	Oid     int64  `json:"oid,omitempty"`
	Rpid    int64  `json:"rpid,omitempty"`
	Root    int64  `json:"root,omitempty"`
	Dialog  int64  `json:"dialog,omitempty"`
	Remark  string `json:"remark,omitempty"`
	Adname  string `json:"adname,omitempty"`
	Mtime   int64  `json:"mtime,omitempty"`
	Action  int8   `json:"action,omitempty"`
	Sort    int8   `json:"sort,omitempty"`
	Tp      int8   `json:"tp,omitempty"`
	Moral   int    `json:"moral,omitempty"`
	Notify  bool   `json:"notify,omitempty"`
	Top     uint32 `json:"top,omitempty"`
	Ftime   int64  `json:"ftime,omitempty"`
	State   int8   `json:"state,omitempty"`
	Audit   int8   `json:"audit,omitempty"`
	Reason  int8   `json:"reason,omitempty"`
	Content string `json:"content,omitempty"`
	FReason int8   `json:"freason,omitempty"`
	Assist  bool   `json:"assist,omitempty"`
	Count   int    `json:"count,omitempty"`
	Floor   int    `json:"floor,omitempty"`
	IsUp    bool   `json:"is_up,omitempty"`
}

func NewKafka() (k *Kafka, cf func(), err error) {
	var (
		cfg      KafkaConfig
		ct       paladin.Map
		kafkaPro kafka.SyncProducer
	)

	if err = paladin.Get("kafka.toml").Unmarshal(&ct); err != nil {
		err = errors.WithStack(err)
		return
	}
	if err = ct.Get("Kafka").UnmarshalTOML(&cfg); err != nil {
		return
	}

	kc := kafka.NewConfig()
	kc.Producer.RequiredAcks = kafka.WaitForAll // Wait for all in-sync replicas to ack the message
	kc.Producer.Retry.Max = 10                  // Retry up to 10 times to produce the message
	kc.Producer.Return.Successes = true
	ver, err := kafka.ParseKafkaVersion("2.4.0")
	if err != nil {
		return
	}
	kc.Version = ver
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
		log.Error("send AddReply(%v) error(%v)", m, err)
	}
	return
}

func (d *dao) RecoverIndex(c context.Context, oid int64, tp, sort int8) {
	data := kafkadata{
		Oid:  oid,
		Tp:   tp,
		Sort: sort,
	}
	dataJson, err := json.Marshal(data)
	if err != nil {
		log.Error("json marshal error(%v)", err)
		return
	}
	m := &kafka.ProducerMessage{
		Key:     kafka.StringEncoder(strconv.FormatInt(oid, 10)),
		Topic:   d.kafkaPub.Topic,
		Headers: []kafka.RecordHeader{{[]byte("action"), []byte("re_idx")}},
		Value:   kafka.ByteEncoder(dataJson),
	}
	if _, _, err := d.kafkaPub.SendMessage(m); err != nil {
		log.Error("send RecoverIndex(:%v) error(%v)", m, err)
	}
	return
}

// RecoverIndexByRoot push event message into kafka.
func (d *dao) RecoverIndexByRoot(c context.Context, oid, root int64, tp int8) {
	data := kafkadata{
		Oid:  oid,
		Tp:   tp,
		Root: root,
	}
	dataJson, err := json.Marshal(data)
	if err != nil {
		log.Error("json marshal error(%v)", err)
		return
	}
	m := &kafka.ProducerMessage{
		Key:     kafka.StringEncoder(strconv.FormatInt(oid, 10)),
		Topic:   d.kafkaPub.Topic,
		Headers: []kafka.RecordHeader{{[]byte("action"), []byte("re_rt_idx")}},
		Value:   kafka.ByteEncoder(dataJson),
	}
	if _, _, err := d.kafkaPub.SendMessage(m); err != nil {
		log.Error("send RecoverRootIndex(:%v) error(%v)", m, err)
	}
	return
}

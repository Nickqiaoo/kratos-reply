package service

import (
	"bytes"
	"context"
	"github.com/Shopify/sarama"
	"github.com/go-kratos/kratos/pkg/log"
	"strconv"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/golang/protobuf/ptypes/empty"
	"reply/internal/dao"
)

const (
	_chLen = 2048
)

var (
	_rpChs   []chan *sarama.ConsumerMessage
	_likeChs []chan *sarama.ConsumerMessage
)

// Service service.
type Service struct {
	ac       *paladin.Map
	dao      dao.Dao
	consumer *cluster.Consumer
}

type KafkacConfig struct {
	Topic   string
	Group   string
	Brokers []string
}

// New new a service and return.
func New(d dao.Dao) (s *Service, cf func(), err error) {
	s = &Service{
		ac:  &paladin.TOML{},
		dao: d,
	}
	cf = s.Close
	err = paladin.Watch("application.toml", s.ac)
	var kafka KafkacConfig
	if err = s.ac.Get("kafka").UnmarshalTOML(&kafka); err != nil {
		return
	}

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	ver, err := sarama.ParseKafkaVersion("2.4.0")
	if err != nil {
		return
	}
	config.Version = ver
	s.consumer, err = cluster.NewConsumer(kafka.Brokers, kafka.Group, []string{kafka.Topic}, config)
	if err != nil {
		panic(err)
	}
	_rpChs = make([]chan *sarama.ConsumerMessage, 10)
	_likeChs = make([]chan *sarama.ConsumerMessage, 10)
	for i := 0; i < 10; i++ {
		_rpChs[i] = make(chan *sarama.ConsumerMessage, _chLen)
		_likeChs[i] = make(chan *sarama.ConsumerMessage, _chLen)
		go s.consumeProc(i)
	}
	go s.messageConsume()

	return
}

func (s *Service) consumeProc(i int) {
	for {
		msg, ok := <-_rpChs[i]
		if !ok {
			log.Info("consumeproc exit")
			return
		}
		action := s.getAction(msg.Headers)
		log.Info("receive message action(%v)", action)
		switch action {
		case "add":
			s.actionAdd(context.Background(), msg.Value)
		}
	}
}

func (s *Service) getAction(headers []*sarama.RecordHeader) (action string) {
	for _, v := range headers {
		if bytes.Equal(v.Key, []byte("action")) {
			return string(v.Value)
		}
	}
	return ""
}

func (s *Service) messageConsume() {
	for {
		select {
		case err := <-s.consumer.Errors():
			log.Error("consumer error(%v)", err)
		case n := <-s.consumer.Notifications():
			log.Info("consumer reblanced(%v)", n)
		case msg, ok := <-s.consumer.Messages():
			if !ok {
				return
			}
			s.consumer.MarkOffset(msg, "")
			oid, err := strconv.ParseInt(string(msg.Key), 10, 64)
			if err != nil {
				continue
			}
			_rpChs[oid%int64(10)] <- msg
		}
	}
}

// Ping ping the resource.
func (s *Service) Ping(ctx context.Context, e *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, s.dao.Ping(ctx)
}

// Close close the resource.
func (s *Service) Close() {
}

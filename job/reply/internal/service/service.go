package service

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/prometheus/common/log"
	"strconv"

	pb "reply/api"
	"reply/internal/dao"
	"github.com/go-kratos/kratos/pkg/conf/paladin"
	cluster "github.com/bsm/sarama-cluster"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/wire"
)

var Provider = wire.NewSet(New, wire.Bind(new(pb.DemoServer), new(*Service)))

const (
	_chLen = 2048
)

var (
	_rpChs   []chan *sarama.ConsumerMessage
	_likeChs []chan *sarama.ConsumerMessage
)

// Service service.
type Service struct {
	ac  *paladin.Map
	dao dao.Dao
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
	if err  = s.ac.Get("kafka").UnmarshalTOML(kafka);err!=nil{
		return
	}

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	s.consumer ,err = cluster.NewConsumer(kafka.Brokers,kafka.Group,[]string{kafka.Topic},config)
	if err!=nil{
		panic(err)
	}
	_rpChs = make([]chan *sarama.ConsumerMessage, c.Job.Proc)
	_likeChs = make([]chan *sarama.ConsumerMessage, c.Job.Proc)
	for i := 0; i < c.Job.Proc; i++ {
		_rpChs[i] = make(chan *sarama.ConsumerMessage, _chLen)
		_likeChs[i] = make(chan *sarama.ConsumerMessage, _chLen)
		go s.consumeProc(i)
	}
	go s.messageConsume()

	return
}

func (s *Service) consumeProc(i int){

}

func (s *Service) messageConsume(){
	for {
		select {
			case err:= <-s.consumer.Errors():
				log.Error("consumer error(%v)",err)
			case n :=<-s.consumer.Notifications():
				log.Info("consumer reblanced(%v)",n)
			case msg, ok := <-s.consumer.Messages():
				if !ok{
					return
				}
				s.consumer.MarkOffset(msg,"")
				oid, err := strconv.ParseInt(string(msg.Key), 10, 64)
				if err != nil {
					continue
				}
				_rpChs[oid%int64(s.c.Job.Proc)] <- msg
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

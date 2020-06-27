package service

import (
	"context"
	"github.com/go-kratos/kratos/pkg/sync/pipeline/fanout"

	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"kratos-reply/internal/dao"

	"github.com/golang/protobuf/ptypes/empty"
)

// Service service.
type Service struct {
	ac        *paladin.Map
	dao       dao.Dao
	sndDefCnt int
	cache     *fanout.Fanout
	rpid      int64
}

// New new a service and return.
func New(d dao.Dao) (s *Service, cf func(), err error) {
	s = &Service{
		ac:    &paladin.TOML{},
		dao:   d,
		cache: fanout.New("cache"),
		rpid:  1,
	}
	cf = s.Close
	err = paladin.Watch("application.toml", s.ac)
	return
}

// Ping ping the resource.
func (s *Service) Ping(ctx context.Context, e *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, s.dao.Ping(ctx)
}

// Close close the resource.
func (s *Service) Close() {
}

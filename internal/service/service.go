package service

import (
	"context"

	pb "kratos-reply/api"
	"kratos-reply/internal/dao"
	"github.com/go-kratos/kratos/pkg/conf/paladin"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/wire"
)

var Provider = wire.NewSet(New, wire.Bind(new(pb.ReplyServer), new(*Service)))

// Service service.
type Service struct {
	ac  *paladin.Map
	dao dao.Dao
}

// New new a service and return.
func New(d dao.Dao) (s *Service, cf func(), err error) {
	s = &Service{
		ac:  &paladin.TOML{},
		dao: d,
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

// AddReply add a reply.
func (s *Service) AddReply(c context.Context, mid, oid int64, tp, plat int8, ats []int64, accessKey, cookie, captcha, msg, dev, ver, platform string, build int64, buvid string) (r *model.Reply, uri string, err error) {
}

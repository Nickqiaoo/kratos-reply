package service

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/pkg/conf/paladin"
	xtime "github.com/go-kratos/kratos/pkg/time"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/wire"

	pb "kratos-reply/api"
	"kratos-reply/internal/dao"
	"kratos-reply/internal/model"
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
func (s *Service) AddReply(c context.Context, mid, oid int64, tp int8, ats []int64, msg string) (r *model.Reply, err error) {
	var rootID, parentID, dialog int64
	r, err = s.persistReply(c, mid, rootID, parentID, tp, ats, msg, subject, dialog)
	return 
}

// AddReplyReply add reply to a root reply.
func (s *Service) AddReplyReply(c context.Context, mid, oid int64, tp int8, ats []int64, msg string) (r *model.Reply, err error) {
	
}

func (s *Service) persistReply(c context.Context, mid, root, parent int64, tp int8, ats []int64, msg, subject *reply.Subject, dialog int64) (r *model.Reply, err error) {
	rpID, err := s.nextID(c)
	if err != nil {
		return
	}
	// 一级子评论
	if root == parent && root != 0 {
		dialog = rpID
	} else if root != parent {
		parentRp, err := s.reply(c, mid, subject.Oid, parent, tp)
		if err != nil {
			return nil, err
		}
		dialog = parentRp.Dialog
	}
	cTime := xtime.Time(time.Now().Unix())
	r = &model.Reply{
		RpID:   rpID,
		Oid:    subject.Oid,
		Type:   tp,
		Mid:    mid,
		Root:   root,
		State:  model.ReplyStateNormal,
		Parent: parent,
		CTime:  cTime,
		Dialog: dialog,
		Content: &model.Content{
			RpID:    rpID,
			Message: msg,
			Ats:     ats,
			CTime:   cTime,
		},
	}
	
	s.dao.Databus.AddReply(c, subject.Oid, r)
}

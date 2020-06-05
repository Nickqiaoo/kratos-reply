package service

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/pkg/log"
	xtime "github.com/go-kratos/kratos/pkg/time"
	"kratos-reply/internal/model"
)

// AddReply add a reply.
func (s *Service) AddReply(c context.Context, mid, oid int64, tp int8, ats []int64, msg string) (r *model.Reply, err error) {
	var (
		rootID, parentID, dialog int64
		subject                  *model.Subject
	)
	subject, err = s.Subject(c, oid, tp)
	if err != nil {
		return
	}
	r, err = s.persistReply(c, mid, rootID, parentID, tp, ats, msg, subject, dialog)
	return
}

// AddReplyReply add reply to a root reply.
func (s *Service) AddReplyReply(c context.Context, mid, oid int64, tp int8, ats []int64, msg string) (r *model.Reply, err error) {

}

func (s *Service) persistReply(c context.Context, mid, root, parent int64, tp int8, ats []int64, msg string, subject *model.Subject, dialog int64) (r *model.Reply, err error) {
	rpID, err := s.nextID(c)
	if err != nil {
		return
	}
	// 一级子评论
	if root == parent && root != 0 {
		dialog = rpID
	} else if root != parent {
		parentRp, err := s.GetReply(c, subject.Oid, parent, tp)
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
	return
}

// GetReply GetReply
func (s *Service) GetReply(c context.Context, oid, rpID int64, tp int8) (*model.Reply, error) {
	r, err := s.dao.CacheReply(c, rpID)
	if err != nil {
		log.Error("replyCacheDao.GetReply(%d, %d, %d) error(%v)", oid, rpID, tp, err)
		err = nil // NOTE ignore error
	}
	if r == nil {
		r, err = s.dao.Reply.Get(c, oid, rpID)
		if err != nil {
			log.Error("s.reply.GetReply(%d, %d) error(%v)", oid, rpID, err)
			return nil, err
		}
		if r == nil {
			return nil, ecode.ReplyNotExist
		}
		if r.Oid != oid {
			log.Warn("reply dismatches with parameter, oid: %d, rpID: %d, tp: %d, actual: %d, %d, %d", oid, rpID, tp, r.Oid, r.RpID, r.Type)
			return nil, ecode.RequestErr
		}
	}
	return r, nil
}


// Subject get normal state reply subject
func (s *Service) Subject(c context.Context, oid int64, tp int8) (*model.Subject, error) {
	subject, err := s.dao.Subject(c, oid, tp)
	if err != nil {
		return nil, err
	}
	if subject.State == model.SubStateForbid {
		return nil, ecode.ReplyForbidReply
	}
	return subject, nil
}

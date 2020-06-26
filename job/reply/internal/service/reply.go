package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-kratos/kratos/pkg/log"
	"reply/internal/model"
)

func (s *Service) actionAdd(c context.Context, msg []byte) {
	var rp *model.Reply
	if err := json.Unmarshal(msg, &rp); err != nil {
		log.Error("json.Unmarshal() error(%v)", err)
		return
	}
	if rp.RpID == 0 || rp.Oid == 0 || rp.Content == nil {
		log.Error("The structure of reply(%s) from rpCh was wrong", msg)
		return
	}
	if rp.Root == 0 && rp.Parent == 0 {
		s.addReply(c, rp)
	} else {
		//s.addReplyReply(c, rp)
	}
}

func (s *Service) addReply(c context.Context, rp *model.Reply) {
	var (
		err error
		ok  bool
	)
	sub, err := s.dao.RawSubject(c, rp.Oid, rp.Type)
	if err != nil {
		log.Error("s.getSubject failed , oid(%d,%d) err(%v)", rp.Oid, rp.Type, err)
		return
	}
	if sub == nil {
		log.Error("get subject is nil oid(%d) type(%d)", rp.Oid, rp.Type)
		return
	}
	// init some field
	sub.RCount = sub.RCount + 1
	sub.ACount = sub.ACount + 1
	sub.Count = sub.Count + 1
	rp.Floor = sub.Count

	rp.MTime = rp.CTime
	rp.Content.RpID = rp.RpID
	rp.Content.CTime = rp.CTime
	rp.Content.MTime = rp.MTime
	//if len(rp.Content.Ats) == 0 {
	//	rp.Content.Ats = s.regAt(c, rp.Content.Message, 0, rp.Mid)
	//}
	//rp.Content.Topics = s.regTopic(c, rp.Content.Message)
	// begin transaction
	if err = s.tranAdd(c, rp, true); err != nil {
		log.Error("Transaction add reply(%v) error(%v)", rp, err)
		return
	}
	// add cache
	if err = s.dao.AddCacheSubject(c, sub.Oid, sub, sub.Type); err != nil {
		log.Error("s.dao.Mc.AddSubject failed , oid(%d) err(%v)", sub.Oid, err)
	}
	if err = s.dao.AddCacheReply(c, rp.RpID, rp); err != nil {
		log.Error("s.dao.Mc.AddReply failed , RpID(%d) err(%v)", rp.RpID, err)
	}
	// add index cache
	if ok, err = s.dao.ExpireIndex(c, sub.Oid, sub.Type, model.SortByFloor); err == nil && ok {
		if err = s.dao.AddFloorIndex(c, sub.Oid, sub.Type, rp); err != nil {
			log.Error("s.dao.Redis.AddFloorIndex failed , oid(%d) type(%d) err(%v)", sub.Oid, sub.Type, err)
		}
	}
	if ok, err = s.dao.ExpireIndex(c, sub.Oid, sub.Type, model.SortByCount); err == nil && ok {
		if err = s.dao.AddCountIndex(c, sub.Oid, sub.Type, rp); err != nil {
			log.Error("s.dao.Redis.AddCountIndex failed , oid(%d) type(%d) err(%v)", sub.Oid, sub.Type, err)
		}
	}
	if ok, err = s.dao.ExpireIndex(c, sub.Oid, sub.Type, model.SortByLike); err == nil && ok {
		if err = s.dao.AddLikeIndex(c, sub.Oid, sub.Type, rp); err != nil {
			log.Error("s.dao.Redis.AddLikeIndex failed , oid(%d) type(%d) err(%v)", sub.Oid, sub.Type, err)
		}
	}
}

func (s *Service) tranAdd(c context.Context, rp *model.Reply, isRoot bool) (err error) {
	tx, err := s.dao.BeginTran(c)
	if err != nil {
		log.Error("reply(%s) beginTran error(%v)", rp, err)
		return
	}
	var rows int64
	defer func() {
		if err == nil && rows == 0 {
			err = errors.New("sql: transaction add reply failed")
		}
	}()
	if isRoot {
		rows, err = s.dao.TxIncrSubjectCount(tx, rp.Oid, rp.Type, rp.CTime.Time())
		if err != nil || rows == 0 {
			tx.Rollback()
			log.Error("dao.Subject.TxIncrSubjectCount(%v) error(%v) or rows==0", rp, err)
			return
		}
	} else {
		//var rootReply *model.Reply
		//if rootReply, err = s.dao.GetReplyForUpdate(tx, rp.Oid, rp.Root); err != nil {
		//	tx.Rollback()
		//	return err
		//}
		//if rootReply.IsDeleted() {
		//	return fmt.Errorf("the root reply is deleted(%d,%d,%d)", rp.Oid, rp.Type, rp.Root)
		//}

		rows, err = s.dao.TxIncrReplyCount(tx, rp.Oid, rp.Root, rp.CTime.Time())
		if err != nil || rows == 0 {
			tx.Rollback()
			log.Error("dao.Reply.TxIncrSubjectCount(%v) error(%v) or rows==0", rp, err)
			return
		}
		rows, err = s.dao.TxIncrSubjectACount(tx, rp.Oid, rp.Type, 1, rp.CTime.Time())
		if err != nil || rows == 0 {
			tx.Rollback()
			log.Error("dao.Subject.TxIncrACount(%v) error(%v) or rows==0", rp, err)
			return
		}
	}

	rows, err = s.dao.TxContentInsert(tx, rp.Oid, rp.Content)
	if err != nil || rows == 0 {
		tx.Rollback()
		log.Error("dao.Content.TxInContent(%v) error(%v) or rows==0", rp, err)
		return
	}
	rows, err = s.dao.TxReplyInsert(tx, rp)
	if err != nil || rows == 0 {
		tx.Rollback()
		log.Error("dao.Reply.TxInReply(%v) error(%v) or rows==0", rp, err)
		return
	}
	return tx.Commit()
}

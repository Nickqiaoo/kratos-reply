package service

import (
	"context"
	"time"

	"kratos-reply/internal/model"

	"github.com/go-kratos/kratos/pkg/ecode"
	"github.com/go-kratos/kratos/pkg/log"
	xtime "github.com/go-kratos/kratos/pkg/time"
)

var (
	_emptyReplies = make([]*model.Reply, 0)
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

	s.dao.AddReply(c, subject.Oid, r)
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
		r, err = s.dao.RawReply(c, oid, rpID)
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

func (s *Service) nextID(c context.Context) (int64, error) {
	return 1, nil
}

func (s *Service) RootReplies(c context.Context, param *model.PageParam) (page *model.PageResult, err error) {
	subject, err := s.Subject(c, param.Oid, param.Type)
	if err != nil {
		return
	}
	roots, seconds, total, err := s.rootReplies(c, subject, param.Mid, param.Sort, param.Pn, param.Ps, 1, s.sndDefCnt)
}

func (s *Service) rootReplies(c context.Context, subject *model.Subject, mid int64, sort int8, pn, ps, secondPn, secondPs int) (roots, seconds []*model.Reply, total int, err error) {
	var (
		rootMap map[int64]*model.Reply
	)
	// get root replies
	rootIDs, total, err := s.rootReplyIDs(c, subject, sort, pn, ps)
	if err != nil {
		return
	}
	if len(rootIDs) > 0 {
		if rootMap, err = s.repliesMap(c, subject.Oid, subject.Type, rootIDs); err != nil {
			return
		}
	}
	if len(rootIDs) == 0 {
		roots = _emptyReplies
		return
	}
	// get second replies
	secondMap, seconds, err := s.secondReplies(c, subject, rootMap, mid, secondPn, secondPs)
	if err != nil {
		return
	}
	for _, rootID := range rootIDs {
		if rp, ok := rootMap[rootID]; ok {
			if rp.Replies, ok = secondMap[rp.RpID]; !ok {
				rp.Replies = _emptyReplies
			}
			roots = append(roots, rp)
		}
	}
	if roots == nil {
		roots = _emptyReplies
	}
	return
}

func (s *Service) rootReplyIDs(c context.Context, subject *model.Subject, sort int8, pn, ps int) (rpIDs []int64, count int, err error) {
	var (
		ok    bool
		start = (pn - 1) * ps
		end   = start + ps - 1
	)
	if count = subject.RCount; start >= count {
		return
	}
	if ok, err = s.dao.ExpireIndex(c, subject.Oid, subject.Type, sort); err != nil {
		log.Error("s.dao.ExpireIndex(%d,%d,%d) error(%v)", subject.Oid, subject.Type, sort, err)
		return
	}
	if !ok {
		switch sort {
		case model.SortByFloor:
			s.dao.RecoverFloorIdx(c, subject.Oid, subject.Type, end+1, false)
			rpIDs, err = s.dao.GetIdsSortFloor(c, subject.Oid, subject.Type, start, ps)
		case model.SortByCount:
			s.dao.RecoverIndex(c, subject.Oid, subject.Type, sort)
			rpIDs, err = s.dao.GetIdsSortCount(c, subject.Oid, subject.Type, start, ps)
		case model.SortByLike:
			s.dao.RecoverIndex(c, subject.Oid, subject.Type, sort)
			if rpIDs, err = s.dao.GetIdsSortLike(c, subject.Oid, subject.Type, start, ps); err != nil {
				return
			}
		}
		if err != nil {
			log.Error("s.rootIDs(%d,%d,%d,%d,%d) error(%v)", subject.Oid, subject.Type, sort, start, ps, err)
			return
		}
	} else {
		if rpIDs, _, err = s.dao.Range(c, subject.Oid, subject.Type, sort, start, end); err != nil {
			log.Error("s.dao.Redis.Range(%d,%d,%d,%d,%d) error(%v)", subject.Oid, subject.Type, sort, start, end, err)
			return
		}
	}
	return
}

func (s *Service) repliesMap(c context.Context, oid int64, tp int8, rpIDs []int64) (res map[int64]*model.Reply, err error) {
	if len(rpIDs) == 0 {
		return
	}
	var missIDs []int64
	res, err = s.dao.CacheReplies(c, rpIDs)
	if err != nil {
		log.Error("s.dao.Mc.GetMultiReply(%d,%d,%d) error(%v)", oid, tp, rpIDs, err)
		err = nil
		res = make(map[int64]*model.Reply, len(rpIDs))
		missIDs = rpIDs
	}
	for _, key := range rpIDs {
		if (res == nil) || (res[key] == nil) {
			missIDs = append(missIDs, key)
		}
	}
	if len(missIDs) > 0 {
		var (
			mrp map[int64]*model.Reply
			mrc map[int64]*model.Content
		)
		if mrp, err = s.dao.GetReplyByIds(c, oid, tp, missIDs); err != nil {
			log.Error("s.reply.GetReplyByIds(%d,%d,%d) error(%v)", oid, tp, rpIDs, err)
			return
		}
		if mrc, err = s.dao.GetContentByIds(c, oid, missIDs); err != nil {
			log.Error("s.content.GetByIds(%d,%d) error(%v)", oid, rpIDs, err)
			return
		}
		rs := make([]*model.Reply, 0, len(missIDs))
		for _, rpID := range missIDs {
			if rp, ok := mrp[rpID]; ok {
				rp.Content = mrc[rpID]
				res[rpID] = rp
				rs = append(rs, rp.Clone())
			}
		}
		// asynchronized add reply cache
		select {
		case s.replyChan <- replyChan{rps: rs}:
		default:
			log.Warn("s.replyChan is full")
		}
	}
	return
}

func (s *Service) secondReplies(c context.Context, sub *model.Subject, rootMap map[int64]*model.Reply, mid int64, pn, ps int) (res map[int64][]*model.Reply, rs []*model.Reply, err error) {
	var (
		rootIDs, secondIDs []int64
		secondIdxMap       map[int64][]int64
		secondMap          map[int64]*model.Reply
	)
	for rootID, info := range rootMap {
		if info.RCount > 0 {
			rootIDs = append(rootIDs, rootID)
		}
	}
	if len(rootIDs) > 0 {
		if secondIdxMap, secondIDs, err = s.getIdsByRoots(c, sub.Oid, rootIDs, sub.Type, pn, ps); err != nil {
			return
		}
		if secondMap, err = s.repliesMap(c, sub.Oid, sub.Type, secondIDs); err != nil {
			return
		}
	}

	res = make(map[int64][]*model.Reply, len(secondIdxMap))
	for root, idxs := range secondIdxMap {
		seconds := make([]*model.Reply, 0, len(idxs))
		for _, rpid := range idxs {
			if r, ok := secondMap[rpid]; ok {
				seconds = append(seconds, r)
			}
		}
		res[root] = seconds
		rs = append(rs, seconds...)
	}
	return
}

func (s *Service) getIdsByRoots(c context.Context, oid int64, roots []int64, tp int8, pn, ps int) (sidsmap map[int64][]int64, ids []int64, err error) {
	var (
		start    = (pn - 1) * ps
		end      = start + ps - 1
		miss     []int64
		tmprpIDs []int64
	)
	if sidsmap, ids, miss, err = s.dao.RangeByRoots(c, roots, start, end); err != nil {
		log.Error("s.dao.Redis.RangeByRoots() err(%v)", err)
		return
	}
	if len(miss) == 0 {
		return
	}
	for _, root := range miss {
		if tmprpIDs, err = s.dao.GetIdsByRoot(c, oid, root, tp, start, ps); err != nil {
			log.Error("s.dao.Reply.GetIdsByRoot(oid %d,tp %d,root %d) err(%v)", oid, tp, root, err)
		}
		if len(tmprpIDs) != 0 {
			sidsmap[root] = tmprpIDs
			ids = append(ids, tmprpIDs...)
			s.dao.RecoverIndexByRoot(c, oid, root, tp)
		}
	}
	return
}

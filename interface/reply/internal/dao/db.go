package dao

import (
	"context"
	"fmt"
	"kratos-reply/internal/util"

	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/go-kratos/kratos/pkg/database/sql"
	"github.com/go-kratos/kratos/pkg/log"
	"kratos-reply/internal/model"
)

const (
	_replySharding int64 = 200
)

func (d *dao) hit(oid int64) int64 {
	return 0
	//return oid % _replySharding
}

const (
	_selContsSQL = "SELECT rpid,message,ats,ip,plat,device,topics FROM reply_content_%d WHERE rpid IN (%s)"

	_selByIdsSQL   = "SELECT id,oid,type,mid,root,parent,dialog,count,rcount,`like`,hate,floor,state,attr,ctime,mtime FROM reply_%d WHERE id IN (%s)"
	_selSQL        = "SELECT id,oid,type,mid,root,parent,dialog,count,rcount,`like`,hate,floor,state,attr,ctime,mtime FROM reply_%d WHERE id=?"
	_selSubjectSQL = "SELECT oid,type,mid,count,rcount,acount,state,attr,ctime,mtime,meta FROM reply_subject_%d WHERE oid=? AND type=?"

	_selIdsByRootStateSQL = "SELECT id FROM reply_%d WHERE oid=? AND type=? AND root=? AND state in (0,1,2,5,6) ORDER BY floor limit ?,?"
	_selIdsByFloorSQL     = "SELECT id FROM reply_%d WHERE oid=? AND type=? AND root=0 AND state in (0,1,2,5,6) ORDER BY floor DESC limit ?,?"
	_selIdsByCountSQL     = "SELECT id FROM reply_%d WHERE oid=? AND type=? AND root=0 AND state in (0,1,2,5,6) ORDER BY rcount DESC limit ?,?"
	_selIdsByLikeSQL      = "SELECT id FROM reply_%d WHERE oid=? AND type=? AND root=0 AND state in (0,1,2,5,6) ORDER BY `like` DESC limit ?,?" // like >= 3
)

func NewDB() (db *sql.DB, cf func(), err error) {
	var (
		cfg sql.Config
		ct  paladin.TOML
	)
	if err = paladin.Get("db.toml").Unmarshal(&ct); err != nil {
		return
	}
	if err = ct.Get("Client").UnmarshalTOML(&cfg); err != nil {
		return
	}
	db = sql.NewMySQL(&cfg)
	cf = func() { db.Close() }
	return
}

func (d *dao) RawSubject(c context.Context, oid int64, tp int8) (sub *model.Subject, err error) {
	// get data from db
	sub = &model.Subject{}
	row := d.db.QueryRow(c, fmt.Sprintf(_selSubjectSQL, d.hit(oid)), oid, tp)
	if err = row.Scan(&sub.Oid, &sub.Type, &sub.Mid, &sub.Count, &sub.RCount, &sub.ACount, &sub.State, &sub.Attr, &sub.CTime, &sub.MTime, &sub.Meta); err != nil {
		if err == sql.ErrNoRows {
			sub = nil
			err = nil
		} else {
			log.Error("row.Scan error(%v)", err)
		}
	}
	return
}

func (d *dao) RawReply(c context.Context, oid, rpID int64) (r *model.Reply, err error) {
	r = &model.Reply{}
	row := d.db.QueryRow(c, fmt.Sprintf(_selSQL, d.hit(oid)), rpID)
	if err = row.Scan(&r.RpID, &r.Oid, &r.Type, &r.Mid, &r.Root, &r.Parent, &r.Dialog, &r.Count, &r.RCount, &r.Like, &r.Hate, &r.Floor, &r.State, &r.Attr, &r.CTime, &r.MTime); err != nil {
		if err == sql.ErrNoRows {
			r = nil
			err = nil
		} else {
			log.Error("row.Scan error(%v)", err)
		}
	}
	return
}

// GetIdsSortFloor limit get reply ids and order by floor desc.
func (d *dao) GetIdsSortFloor(c context.Context, oid int64, tp int8, offset, count int) (res []int64, err error) {
	rows, err := d.db.Query(c, fmt.Sprintf(_selIdsByFloorSQL, d.hit(oid)), oid, tp, offset, count)
	if err != nil {
		log.Error("dao.selIdsByFloorStmt query err(%v)", err)
		return
	}
	defer rows.Close()
	var id int64
	res = make([]int64, 0, count)
	for rows.Next() {
		if err = rows.Scan(&id); err != nil {
			log.Error("rows.scan err is (%v)", err)
			return
		}
		res = append(res, id)
	}
	if err = rows.Err(); err != nil {
		log.Error("rows.err error(%v)", err)
		return
	}
	return
}

// GetIdsSortCount limit get reply ids and order by rcount desc.
func (d *dao) GetIdsSortCount(c context.Context, oid int64, tp int8, offset, count int) (res []int64, err error) {
	rows, err := d.db.Query(c, fmt.Sprintf(_selIdsByCountSQL, d.hit(oid)), oid, tp, offset, count)
	if err != nil {
		log.Error("dao.selIdsByCountStmt query err(%v)", err)
		return
	}
	defer rows.Close()
	var id int64
	res = make([]int64, 0, count)
	for rows.Next() {
		if err = rows.Scan(&id); err != nil {
			log.Error("rows.scan err is (%v)", err)
			return
		}
		res = append(res, id)
	}
	if err = rows.Err(); err != nil {
		log.Error("rows.err error(%v)", err)
		return
	}
	return
}

// GetIdsSortLike limit get reply ids and order by like desc.
func (d *dao) GetIdsSortLike(c context.Context, oid int64, tp int8, offset, count int) (res []int64, err error) {
	rows, err := d.db.Query(c, fmt.Sprintf(_selIdsByLikeSQL, d.hit(oid)), oid, tp, offset, count)
	if err != nil {
		log.Error(" dao.selIdsByLikeStmt query err(%v)", err)
		return
	}
	defer rows.Close()
	var id int64
	res = make([]int64, 0, count)
	for rows.Next() {
		if err = rows.Scan(&id); err != nil {
			log.Error("rows.scan err is (%v)", err)
			return
		}
		res = append(res, id)
	}
	if err = rows.Err(); err != nil {
		log.Error("rows.err error(%v)", err)
		return
	}
	return
}

// GetReplyByIds get replies by reply ids.
func (d *dao) GetReplyByIds(c context.Context, oid int64, tp int8, rpIds []int64) (rpMap map[int64]*model.Reply, err error) {
	if len(rpIds) == 0 {
		return
	}
	rows, err := d.db.Query(c, fmt.Sprintf(_selByIdsSQL, d.hit(oid), util.JoinInts(rpIds)))
	if err != nil {
		log.Error("mysql.Query error(%v)", err)
		return
	}
	defer rows.Close()
	rpMap = make(map[int64]*model.Reply, len(rpIds))
	for rows.Next() {
		r := &model.Reply{}
		if err = rows.Scan(&r.RpID, &r.Oid, &r.Type, &r.Mid, &r.Root, &r.Parent, &r.Dialog, &r.Count, &r.RCount, &r.Like, &r.Hate, &r.Floor, &r.State, &r.Attr, &r.CTime, &r.MTime); err != nil {
			if err == sql.ErrNoRows {
				r = nil
			} else {
				log.Error("row.Scan error(%v)", err)
				return
			}
		}
		rpMap[r.RpID] = r
	}
	if err = rows.Err(); err != nil {
		log.Error("rows.err error(%v)", err)
		return
	}
	return
}

// GetByIds get reply contents by reply ids.
func (d *dao) GetContentByIds(c context.Context, oid int64, rpIds []int64) (rcMap map[int64]*model.Content, err error) {
	if len(rpIds) == 0 {
		return
	}
	rows, err := d.db.Query(c, fmt.Sprintf(_selContsSQL, d.hit(oid), util.JoinInts(rpIds)))
	if err != nil {
		log.Error("contentDao.Query error(%v)", err)
		return
	}
	defer rows.Close()
	rcMap = make(map[int64]*model.Content, len(rpIds))
	for rows.Next() {
		rc := &model.Content{}
		if err = rows.Scan(&rc.RpID, &rc.Message, &rc.Ats, &rc.IP, &rc.Plat, &rc.Device); err != nil {
			log.Error("row.Scan error(%v)", err)
			return
		}
		rcMap[rc.RpID] = rc
	}
	if err = rows.Err(); err != nil {
		log.Error("rows.err error(%v)", err)
		return
	}
	return
}

// GetIdsByRoot limit get reply ids of root reply and order by floor.
func (d *dao) GetIdsByRoot(c context.Context, oid, root int64, tp int8, offset, count int) (res []int64, err error) {
	rows, err := d.db.Query(c, fmt.Sprintf(_selIdsByRootStateSQL, d.hit(oid)), oid, tp, root, offset, count)
	if err != nil {
		log.Error("dao.selIdsByRtStmt,oid(%d),root(%d),tp(%d),offset(%d),query err(%v)", oid, root, tp, offset, err)
		return
	}
	defer rows.Close()
	var id int64
	res = make([]int64, 0, count)
	for rows.Next() {
		if err = rows.Scan(&id); err != nil {
			log.Error("rows.scan err is (%v,%v,%v,%v,%v)", oid, root, offset, count, err)
			return
		}
		res = append(res, id)
	}
	if err = rows.Err(); err != nil {
		log.Error("rows.err error(%v)", err)
		return
	}
	return
}

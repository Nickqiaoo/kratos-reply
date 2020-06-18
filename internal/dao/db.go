package dao

import (
	"context"
	"fmt"

	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/go-kratos/kratos/pkg/database/sql"
	"github.com/go-kratos/kratos/pkg/log"
	"kratos-reply/internal/model"
)

const (
	_replySharding int64 = 200
)

func (d *dao) hit(oid int64) int64 {
	return oid % _replySharding
}

const (
	_selSQL           = "SELECT id,oid,type,mid,root,parent,dialog,count,rcount,`like`,hate,floor,state,attr,ctime,mtime FROM reply_%d WHERE id=?"
	_selSubjectSQL    = "SELECT oid,type,mid,count,rcount,acount,state,attr,ctime,mtime,meta FROM reply_subject_%d WHERE oid=? AND type=?"
	_selIdsByFloorSQL = "SELECT id FROM reply_%d WHERE oid=? AND type=? AND root=0 AND state in (0,1,2,5,6) ORDER BY floor DESC limit ?,?"
	_selIdsByCountSQL = "SELECT id FROM reply_%d WHERE oid=? AND type=? AND root=0 AND state in (0,1,2,5,6) ORDER BY rcount DESC limit ?,?"
	_selIdsByLikeSQL  = "SELECT id FROM reply_%d WHERE oid=? AND type=? AND root=0 AND state in (0,1,2,5,6) ORDER BY `like` DESC limit ?,?" // like >= 3
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

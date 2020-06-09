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

const (
	_selSQL        = "SELECT id,oid,type,mid,root,parent,dialog,count,rcount,`like`,hate,floor,state,attr,ctime,mtime FROM reply_%d WHERE id=?"
	_selSubjectSQL = "SELECT oid,type,mid,count,rcount,acount,state,attr,ctime,mtime,meta FROM reply_subject_%d WHERE oid=? AND type=?"
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

func (d *dao) hit(oid int64) int64 {
	return oid % _replySharding
}

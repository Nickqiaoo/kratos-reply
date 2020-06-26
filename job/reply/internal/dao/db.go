package dao

import (
	"context"
	"fmt"
	"github.com/go-kratos/kratos/pkg/log"
	"time"

	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/go-kratos/kratos/pkg/database/sql"
	"reply/internal/model"
)

const (
	_replySharding int64 = 200
)

func (d *dao) hit(oid int64) int64 {
	return oid % _replySharding
}

const (
	_inSQL           = "INSERT IGNORE INTO reply_%d (id,oid,type,mid,root,parent,dialog,floor,state,attr,ctime,mtime) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
	_incrSubCntSQL   = "UPDATE reply_subject_%d SET count=count+1,rcount=rcount+1,acount=acount+1,mtime=? WHERE oid=? AND type=?"
	_selSQLForUpdate = "SELECT id,oid,type,mid,root,parent,dialog,count,rcount,`like`,hate,floor,state,attr,ctime,mtime FROM reply_%d WHERE id=? for update"
	_incrSubACntSQL  = "UPDATE reply_subject_%d SET acount=acount+?,mtime=? WHERE oid=? AND type=?"
	_inContSQL       = "INSERT IGNORE INTO reply_content_%d (rpid,message,ats,ip,plat,device,version,ctime,mtime,topics) VALUES(?,?,?,?,?,?,?,?,?,?)"

	_selSubjectSQL = "SELECT oid,type,mid,count,rcount,acount,state,attr,ctime,mtime,meta FROM reply_subject_%d WHERE oid=? AND type=?"
	_incrCntSQL    = "UPDATE reply_%d SET count=count+1,rcount=rcount+1,mtime=? WHERE id=?"
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

func (d *dao) RawArticle(ctx context.Context, id int64) (art *model.Article, err error) {
	// get data from db
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

func (d *dao) BeginTran(c context.Context) (*sql.Tx, error) {
	return d.db.Begin(c)
}

// TxIncrSubjectCount incr subject count and rcount by transaction.
func (d *dao) TxIncrSubjectCount(tx *sql.Tx, oid int64, tp int8, now time.Time) (rows int64, err error) {
	res, err := tx.Exec(fmt.Sprintf(_incrSubCntSQL, d.hit(oid)), now, oid, tp)
	if err != nil {
		log.Error("mysqlDB.Exec() error(%v)", err)
		return
	}
	return res.RowsAffected()
}

func (d *dao) GetReplyForUpdate(tx *sql.Tx, oid, rpID int64) (r *model.Reply, err error) {
	r = &model.Reply{}
	row := tx.QueryRow(fmt.Sprintf(_selSQLForUpdate, d.hit(oid)), rpID)
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

// TxIncrReplyCount incr count and rcount of reply by transaction.
func (d *dao) TxIncrReplyCount(tx *sql.Tx, oid, rpID int64, now time.Time) (rows int64, err error) {
	res, err := tx.Exec(fmt.Sprintf(_incrCntSQL, d.hit(oid)), now, rpID)
	if err != nil {
		log.Error("mysqlDB.Exec error(%v)", err)
		return
	}
	return res.RowsAffected()
}

// TxIncrSubjectACount incr subject acount by transaction.
func (d *dao) TxIncrSubjectACount(tx *sql.Tx, oid int64, tp int8, count int, now time.Time) (rows int64, err error) {
	res, err := tx.Exec(fmt.Sprintf(_incrSubACntSQL, d.hit(oid)), count, now, oid, tp)
	if err != nil {
		log.Error("mysqlDB.Exec() error(%v)", err)
		return
	}
	return res.RowsAffected()
}

// TxContentInsert insert reply content by transaction.
func (d *dao) TxContentInsert(tx *sql.Tx, oid int64, rc *model.Content) (rows int64, err error) {
	res, err := tx.Exec(fmt.Sprintf(_inContSQL, d.hit(oid)), rc.RpID, rc.Message, rc.Ats, rc.IP, rc.Plat, rc.Device, rc.Version, rc.CTime, rc.MTime)
	if err != nil {
		log.Error("mysqlDB.Exec error(%v)", err)
		return
	}
	return res.RowsAffected()
}

// TxReplyInsert insert reply by transaction.
func (d *dao) TxReplyInsert(tx *sql.Tx, r *model.Reply) (rows int64, err error) {
	res, err := tx.Exec(fmt.Sprintf(_inSQL, d.hit(r.Oid)), r.RpID, r.Oid, r.Type, r.Mid, r.Root, r.Parent, r.Dialog, r.Floor, r.State, r.Attr, r.CTime, r.MTime)
	if err != nil {
		log.Error("mysqlDB.Exec error(%v)", err)
		return
	}
	return res.RowsAffected()
}

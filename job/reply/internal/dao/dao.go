package dao

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/pkg/cache/memcache"
	"github.com/go-kratos/kratos/pkg/cache/redis"
	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/go-kratos/kratos/pkg/database/sql"
	"github.com/go-kratos/kratos/pkg/sync/pipeline/fanout"
	xtime "github.com/go-kratos/kratos/pkg/time"
	"reply/internal/model"

	"github.com/google/wire"
)

var Provider = wire.NewSet(New, NewDB, NewRedis, NewMC)

//go:generate kratos tool genbts
// Dao dao interface
type Dao interface {
	Close()
	Ping(ctx context.Context) (err error)

	//mysql
	RawSubject(c context.Context, oid int64, tp int8) (sub *model.Subject, err error)
	BeginTran(c context.Context) (*sql.Tx, error)
	TxIncrSubjectCount(tx *sql.Tx, oid int64, tp int8, now time.Time) (rows int64, err error)
	GetReplyForUpdate(tx *sql.Tx, oid, rpID int64) (r *model.Reply, err error)
	TxIncrReplyCount(tx *sql.Tx, oid, rpID int64, now time.Time) (rows int64, err error)
	TxIncrSubjectACount(tx *sql.Tx, oid int64, tp int8, count int, now time.Time) (rows int64, err error)
	TxContentInsert(tx *sql.Tx, oid int64, rc *model.Content) (rows int64, err error)
	TxReplyInsert(tx *sql.Tx, r *model.Reply) (rows int64, err error)

	//redis
	ExpireIndex(ctx context.Context, oid int64, tp, sort int8) (ok bool, err error)
	AddFloorIndex(c context.Context, oid int64, tp int8, rs ...*model.Reply) (err error)
	AddCountIndex(c context.Context, oid int64, tp int8, rp *model.Reply) (err error)
	AddLikeIndex(c context.Context, oid int64, tp int8, r *model.Reply) (err error)

	//memcache
	AddCacheSubject(c context.Context, id int64, val *model.Subject, tp int8) (err error)
	AddCacheReply(c context.Context, rpid int64, reply *model.Reply) (err error)
}

// dao dao.
type dao struct {
	db         *sql.DB
	redis      *redis.Redis
	mc         *memcache.Memcache
	cache      *fanout.Fanout
	demoExpire int32
}

// New new a dao and return.
func New(r *redis.Redis, mc *memcache.Memcache, db *sql.DB) (d Dao, cf func(), err error) {
	return newDao(r, mc, db)
}

func newDao(r *redis.Redis, mc *memcache.Memcache, db *sql.DB) (d *dao, cf func(), err error) {
	var cfg struct {
		DemoExpire xtime.Duration
	}
	if err = paladin.Get("application.toml").UnmarshalTOML(&cfg); err != nil {
		return
	}
	d = &dao{
		db:         db,
		redis:      r,
		mc:         mc,
		cache:      fanout.New("cache"),
		demoExpire: int32(time.Duration(cfg.DemoExpire) / time.Second),
	}
	cf = d.Close
	return
}

// Close close the resource.
func (d *dao) Close() {
	d.cache.Close()
}

// Ping ping the resource.
func (d *dao) Ping(ctx context.Context) (err error) {
	return nil
}

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
	"kratos-reply/internal/model"

	"github.com/google/wire"
)

var Provider = wire.NewSet(New, NewDB, NewRedis, NewMC, NewKafka)

//go:generate kratos tool genbts
// Dao dao interface
type Dao interface {
	Close()
	Ping(ctx context.Context) (err error)
	// bts: -nullcache=&model.Subject{ID:-1} -check_null_code=$!=nil&&$.ID==-1
	Subject(c context.Context, oid int64, tp int8) (*model.Subject, error)

	//memcache
	CacheReply(c context.Context, id int64) (res *model.Reply, err error)
	CacheReplies(c context.Context, ids []int64) (res map[int64]*model.Reply, err error)

	//MySQL
	RawReply(ctx context.Context, oid, rpID int64) (r *model.Reply, err error)
	GetReplyByIds(c context.Context, oid int64, tp int8, rpIds []int64) (rpMap map[int64]*model.Reply, err error)
	GetContentByIds(c context.Context, oid int64, rpIds []int64) (rcMap map[int64]*model.Content, err error)
	GetIdsByRoot(c context.Context, oid, root int64, tp int8, offset, count int) (res []int64, err error)

	//redis
	ExpireIndex(ctx context.Context, oid int64, tp, sort int8) (ok bool, err error)
	Range(ctx context.Context, oid int64, tp, sort int8, start, end int) (rpIds []int64, isEnd bool, err error)
	RangeByRoots(c context.Context, roots []int64, start, end int) (mrpids map[int64][]int64, idx, miss []int64, err error)

	//kafka
	AddReply(c context.Context, oid int64, rp *model.Reply)
	GetIdsSortFloor(c context.Context, oid int64, tp int8, offset, count int) (res []int64, err error)
	GetIdsSortCount(c context.Context, oid int64, tp int8, offset, count int) (res []int64, err error)
	GetIdsSortLike(c context.Context, oid int64, tp int8, offset, count int) (res []int64, err error)
	RecoverIndex(c context.Context, oid int64, tp, sort int8)
	RecoverIndexByRoot(c context.Context, oid, root int64, tp int8)
}

// dao dao.
type dao struct {
	db         *sql.DB
	redis      *redis.Redis
	mc         *memcache.Memcache
	kafkaPub   *Kafka
	cache      *fanout.Fanout
	demoExpire int32
}

// New new a dao and return.
func New(k *Kafka, r *redis.Redis, mc *memcache.Memcache, db *sql.DB) (d Dao, cf func(), err error) {
	return newDao(k, r, mc, db)
}

func newDao(k *Kafka, r *redis.Redis, mc *memcache.Memcache, db *sql.DB) (d *dao, cf func(), err error) {
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
		kafkaPub:   k,
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

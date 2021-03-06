package dao

import (
	"context"
	"strconv"

	"github.com/go-kratos/kratos/pkg/cache/memcache"
	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/go-kratos/kratos/pkg/log"
	"kratos-reply/internal/model"
)

const (
	_prefixSub      = "s_"
	_prefixRp       = "r_"
	_prefixAdminTop = "at_"
	_prefixUpperTop = "ut_"
	_prefixConfig   = "c_%d_%d_%d"
	_prefixCaptcha  = "pc_%d"
)

//go:generate kratos tool genmc
type _mc interface {
	// mc: -key=keySub -type=get
	CacheSubject(c context.Context, oid int64, tp int8) (*model.Subject, error)
	// mc: -key=keySub -expire=d.demoExpire
	AddCacheSubject(c context.Context, oid int64, sub *model.Subject, tp int8) (err error)
	// mc: -key=keySub
	DeleteSubjectCache(c context.Context, oid int64, tp int8) (err error)
	// mc: -key=keyRp -type=get
	CacheReply(c context.Context, rpID int64) (*model.Reply, error)
	// mc: -key=keyRp -type=get
	CacheReplies(c context.Context, rpID []int64) (map[int64]*model.Reply, error)
}

func NewMC() (mc *memcache.Memcache, cf func(), err error) {
	var (
		cfg memcache.Config
		ct  paladin.TOML
	)
	if err = paladin.Get("memcache.toml").Unmarshal(&ct); err != nil {
		return
	}
	if err = ct.Get("Client").UnmarshalTOML(&cfg); err != nil {
		return
	}
	mc = memcache.New(&cfg)
	cf = func() { mc.Close() }
	return
}

func (d *dao) PingMC(ctx context.Context) (err error) {
	if err = d.mc.Set(ctx, &memcache.Item{Key: "ping", Value: []byte("pong"), Expiration: 0}); err != nil {
		log.Error("conn.Set(PING) error(%v)", err)
	}
	return
}

func keySub(oid int64, tp int8) string {
	return _prefixSub + strconv.FormatInt((oid<<8)|int64(tp), 10)
}

func keyRp(rpID int64) string {
	return _prefixRp + strconv.FormatInt(rpID, 10)
}

// AddCacheReply add reply into memcache.
func (d *dao) AddCacheReply(c context.Context, rs ...*model.Reply) (err error) {
	if len(rs) == 0 {
		return
	}
	for _, r := range rs {
		if r == nil {
			continue
		}
		key := keyRp(r.RpID)
		item := &memcache.Item{Key: key, Object: r, Expiration: d.demoExpire, Flags: memcache.FlagJSON}
		if err = d.mc.Set(c, item); err != nil {
			log.Error("conn.Set(%s,%v) error(%v)", key, r, err)
		}
	}
	return
}

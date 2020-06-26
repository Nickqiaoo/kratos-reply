package dao

import (
	"context"
	"strconv"

	"github.com/go-kratos/kratos/pkg/cache/memcache"
	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/go-kratos/kratos/pkg/log"
	"reply/internal/model"
)

const (
	_prefixSub = "s_"
	_prefixRp  = "r_"
)

//go:generate kratos tool genmc
type _mc interface {
	// mc: -key=keySub -expire=d.demoExpire
	AddCacheSubject(c context.Context, oid int64, sub *model.Subject, tp int8) (err error)
	// mc: -key=keyRp -expire=d.demoExpire
	AddCacheReply(c context.Context, rpid int64, reply *model.Reply) (err error)
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

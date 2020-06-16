package dao

import (
	"context"
	"fmt"
	"strconv"

	"github.com/go-kratos/kratos/pkg/cache/redis"
	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/go-kratos/kratos/pkg/log"
)

const (
	_prefixIdx       = "i_"
	_prefixRtIdx     = "ri_"
	_prefixRpt       = "rt_"
	_prefixLike      = "l_"
	_prefixAuditIdx  = "ai_%d_%d"
	_prefixDialogIdx = "d_%d"

	// f_{折叠类型，根评论还是评论区}_{评论区ID或者根评论ID}
	_foldedReplyFmt = "f_%s_%d"

	_prefixSpamRec   = "sr_"
	_prefixSpamDaily = "sd_"
	_prefixSpamAct   = "sa_"
	_prefixTopOid    = "tro_"
)

const (
	_oidOverflow = 1 << 48
)

func keyIdx(oid int64, tp, sort int8) string {
	if oid > _oidOverflow {
		return fmt.Sprintf("%s_%d_%d_%d", _prefixIdx, oid, tp, sort)
	}
	return _prefixIdx + strconv.FormatInt((oid<<16)|(int64(tp)<<8)|int64(sort), 10)
}

func NewRedis() (r *redis.Redis, cf func(), err error) {
	var (
		cfg redis.Config
		ct  paladin.Map
	)
	if err = paladin.Get("redis.toml").Unmarshal(&ct); err != nil {
		return
	}
	if err = ct.Get("Client").UnmarshalTOML(&cfg); err != nil {
		return
	}
	r = redis.NewRedis(&cfg)
	cf = func() { r.Close() }
	return
}

func (d *dao) PingRedis(ctx context.Context) (err error) {
	if _, err = d.redis.Do(ctx, "SET", "ping", "pong"); err != nil {
		log.Error("conn.Set(PING) error(%v)", err)
	}
	return
}

func (d *dao) ExpireIndex(ctx context.Context, oid int64, tp, sort int8) (ok bool, err error) {
	if ok, err = redis.Bool(d.redis.Do(ctx, "EXPIRE", keyIdx(oid, tp, sort), dao.expireRdsIdx)); err != nil {
		log.Error("conn.Do(EXPIRE) error(%v)", err)
	}
	return
}

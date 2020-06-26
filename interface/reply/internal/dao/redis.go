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

func keyRtIdx(rpID int64) string {
	return _prefixRtIdx + strconv.FormatInt(rpID, 10)
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
	if ok, err = redis.Bool(d.redis.Do(ctx, "EXPIRE", keyIdx(oid, tp, sort), d.demoExpire)); err != nil {
		log.Error("conn.Do(EXPIRE) error(%v)", err)
	}
	return
}

func (d *dao) Range(ctx context.Context, oid int64, tp, sort int8, start, end int) (rpIds []int64, isEnd bool, err error) {
	key := keyIdx(oid, tp, sort)
	values, err := redis.Values(d.redis.Do(ctx, "ZREVRANGE", key, start, end))
	if err != nil {
		log.Error("conn.Do(ZREVRANGE, %s) error(%v)", key, err)
		return
	}
	if len(values) == 0 {
		return
	}
	err = redis.ScanSlice(values, &rpIds)
	if len(rpIds) > 0 && rpIds[len(rpIds)-1] == -1 {
		rpIds = rpIds[:len(rpIds)-1]
		isEnd = true
	}
	return
}

// RangeByRoots range roots's replyies.
func (d *dao) RangeByRoots(c context.Context, roots []int64, start, end int) (mrpids map[int64][]int64, idx, miss []int64, err error) {
	conn := d.redis.Conn(c)
	defer conn.Close()
	for _, root := range roots {
		// if exist delay expire time
		if err = conn.Send("EXPIRE", keyRtIdx(root), d.demoExpire); err != nil {
			log.Error("conn.Send(EXPIRE) err(%v)", err)
			return
		}
		if err = conn.Send("ZRANGE", keyRtIdx(root), start, end); err != nil {
			log.Error("conn.Send(ZRANGE) err(%v)", err)
			return
		}
	}
	if err = conn.Flush(); err != nil {
		log.Error("conn.SEND(FLUSH) err(%v)", err)
		return
	}
	mrpids = make(map[int64][]int64, len(roots))
	for _, root := range roots {
		var (
			rpids  []int64
			values []interface{}
		)
		if _, err = conn.Receive(); err != nil {
			log.Error("redis.Bool() err(%v)", err)
			return
		}
		if values, err = redis.Values(conn.Receive()); err != nil {
			log.Error("redis.Values() err(%v)", err)
			return
		}
		if len(values) == 0 {
			miss = append(miss, root)
			continue
		}
		if err = redis.ScanSlice(values, &rpids); err != nil {
			log.Error("redis.ScanSlice() err(%v) ", err)
			return
		}
		idx = append(idx, rpids...)
		mrpids[root] = rpids
	}
	return
}

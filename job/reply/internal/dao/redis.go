package dao

import (
	"context"
	"fmt"
	"reply/internal/model"
	"strconv"

	"github.com/go-kratos/kratos/pkg/cache/redis"
	"github.com/go-kratos/kratos/pkg/conf/paladin"
	"github.com/go-kratos/kratos/pkg/log"
)

const (
	_prefixIdx   = "i_"
	_prefixRtIdx = "ri_"
)

const (
	_maxCount    = 20000
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
	if ok, err = redis.Bool(d.redis.Do(ctx, "EXPIRE", keyIdx(oid, tp, sort), d.demoExpire)); err != nil {
		log.Error("conn.Do(EXPIRE) error(%v)", err)
	}
	return
}

// AddFloorIndex add index by floor.
func (d *dao) AddFloorIndex(c context.Context, oid int64, tp int8, rs ...*model.Reply) (err error) {
	if len(rs) == 0 {
		return
	}
	key := keyIdx(oid, tp, model.SortByFloor)
	conn := d.redis.Conn(c)
	defer conn.Close()
	for _, r := range rs {
		if err = conn.Send("ZADD", key, r.Floor, r.RpID); err != nil {
			log.Error("conn.Send error(%v)", err)
			return
		}
	}
	if err = conn.Send("EXPIRE", key, d.demoExpire); err != nil {
		log.Error("conn.Send error(%v)", err)
		return
	}
	if err = conn.Flush(); err != nil {
		log.Error("conn.Flush error(%v)", err)
		return
	}
	for i := 0; i < len(rs)+1; i++ {
		if _, err = conn.Receive(); err != nil {
			log.Error("conn.Receive() error(%v)", err)
			return
		}
	}
	return
}

// AddCountIndex add index by count.
func (d *dao) AddCountIndex(c context.Context, oid int64, tp int8, rp *model.Reply) (err error) {
	var count int
	if count, err = d.CountReplies(c, oid, tp, model.SortByCount); err != nil {
		return
	} else if count >= _maxCount {
		var min int
		if min, err = d.MinScore(c, oid, tp, model.SortByCount); err != nil {
			return
		}
		if rp.RCount <= min {
			return
		}
	}

	key := keyIdx(oid, tp, model.SortByCount)
	conn := d.redis.Conn(c)
	defer conn.Close()
	if err = conn.Send("ZADD", key, int64(rp.RCount)<<32|(int64(rp.Floor)&0xFFFFFFFF), rp.RpID); err != nil {
		log.Error("conn.Send error(%v)", err)
		return
	}
	if err = conn.Send("EXPIRE", key, d.demoExpire); err != nil {
		log.Error("conn.Send error(%v)", err)
		return
	}
	if err = conn.Flush(); err != nil {
		log.Error("conn.Flush error(%v)", err)
		return
	}
	for i := 0; i < 2; i++ {
		if _, err = conn.Receive(); err != nil {
			log.Error("conn.Receive() error(%v)", err)
			return
		}
	}
	return
}

// CountReplies get count of reply.
func (d *dao) CountReplies(c context.Context, oid int64, tp, sort int8) (count int, err error) {
	key := keyIdx(oid, tp, sort)
	if count, err = redis.Int(d.redis.Do(c, "ZCARD", key)); err != nil {
		log.Error("CountReplies error(%v)", err)
	}
	return
}

// MinScore get the lowest score from sorted set
func (d *dao) MinScore(c context.Context, oid int64, tp int8, sort int8) (score int, err error) {
	key := keyIdx(oid, tp, sort)
	values, err := redis.Values(d.redis.Do(c, "ZRANGE", key, 0, 0, "WITHSCORES"))
	if err != nil {
		log.Error("conn.Do(ZREVRANGE, %s) error(%v)", key, err)
		return
	}
	if len(values) != 2 {
		err = fmt.Errorf("redis zrange items(%v) length not 2", values)
		return
	}
	var id int64
	redis.Scan(values, &id, &score)
	return
}

// AddLikeIndex add index by like.
func (d *dao) AddLikeIndex(c context.Context, oid int64, tp int8, r *model.Reply) (err error) {
	score := int64(float32(r.Like) / float32(r.Hate+r.Count+r.Like) * 100)
	score = score<<32 | (int64(r.RCount) & 0xFFFFFFFF)
	key := keyIdx(oid, tp, model.SortByLike)
	var count int
	if count, err = d.CountReplies(c, oid, tp, model.SortByLike); err != nil {
		return
	} else if count >= _maxCount {
		var min int
		if min, err = d.MinScore(c, oid, tp, model.SortByLike); err != nil {
			return
		}
		if score <= int64(min) {
			return
		}
	}

	conn := d.redis.Conn(c)
	defer conn.Close()
	if err = conn.Send("ZADD", key, score, r.RpID); err != nil {
		log.Error("conn.Send error(%v)", err)
		return
	}
	if err = conn.Send("EXPIRE", key, d.demoExpire); err != nil {
		log.Error("conn.Send error(%v)", err)
		return
	}
	if err = conn.Flush(); err != nil {
		log.Error("conn.Flush error(%v)", err)
		return
	}
	for i := 0; i < 2; i++ {
		if _, err = conn.Receive(); err != nil {
			log.Error("conn.Receive() error(%v)", err)
			return
		}
	}
	return
}

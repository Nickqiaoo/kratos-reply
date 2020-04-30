package http

import (
	"strconv"

	"github.com/go-kratos/kratos/pkg/ecode"
	"github.com/go-kratos/kratos/pkg/log"
	bm "github.com/go-kratos/kratos/pkg/net/http/blademaster"
	"kratos-reply/internal/model"
	"kratos-reply/internal/util"
)

func addReply(c *bm.Context) {
	var (
		err          error
		rp           *model.Reply
		ats          []int64
		root, parent int64
	)

	parm := new(struct {
		Mid    int64  `form:"mid" validate:"min=1,required"`
		Oid    int64  `form:"oid" validate:"min=1,required"`
		Type   int8   `form:"type" validate:"required"`
		Msg    string `form:"message" validate:"required"`
		Root   int64  `form:"root" validate:"required"`
		Parent int64  `form:"parent" validate:"required"`
		AtStr  string `form:"at" validate:"required"`
	})
	if err = c.Bind(parm); err != nil {
		c.JSON(nil, ecode.RequestErr)
		return
	}

	if !((parm.Root == 0 && parm.Parent == 0) || (parm.Root > 0 && parm.Parent > 0)) {
		log.Warn("the wrong root(%d) and parent(%d)", root, parent)
		err = ecode.RequestErr
		c.JSON(nil, err)
		return
	}

	ats, err = util.SplitInts(parm.AtStr)
	if err != nil {
		log.Warn("utils.SplitInts(%s) error(%v)", parm.AtStr, err)
		err = ecode.RequestErr
		c.JSON(nil, err)
		return
	}

	if len(ats) > 10 {
		log.Warn("too many people to be at len(%d)", len(ats))
		err = ecode.ReplyTooManyAts
		c.JSON(nil, err)
		return
	}

	if root == 0 && parent == 0 {
		rp, captchaURL, err = rpSvr.AddReply(c, mid, oid, int8(tp), int8(plat), ats, ak, c.Request.Header.Get("Cookie"), captcha, msg, device, version, platform, build, buvid)
	} else {
		rp, captchaURL, err = rpSvr.AddReplyReply(c, mid, oid, root, parent, int8(tp), int8(plat), ats, ak, c.Request.Header.Get("Cookie"), captcha, msg, device, version, platform, build, buvid)
	}
	if err != nil && err != ecode.ReplyMosaicByFilter {
		log.Warn("rpSvr.AddReply or ReplyReply failed mid(%d) oid(%d) error(%d)", mid, oid, err)
		data := map[string]interface{}{
			"need_captcha": (captchaURL != ""),
			"url":          captchaURL,
		}
		c.JSON(data, err)
		return
	}
	data := map[string]interface{}{
		"rpid":       rp.RpID,
		"rpid_str":   strconv.FormatInt(rp.RpID, 10),
		"dialog":     rp.Dialog,
		"dialog_str": strconv.FormatInt(rp.Dialog, 10),
		"root":       rp.Root,
		"root_str":   strconv.FormatInt(rp.Root, 10),
		"parent":     rp.Parent,
		"parent_str": strconv.FormatInt(rp.Parent, 10),
	}
	c.JSON(data, nil)
}
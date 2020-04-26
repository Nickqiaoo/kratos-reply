package http

import (
	"strconv"

	"kratos-reply/internal/model"
	bm "github.com/go-kratos/kratos/pkg/net/http/blademaster"
	"github.com/go-kratos/kratos/pkg/log"
	"github.com/go-kratos/kratos/pkg/ecode"
)


func addReply(c *bm.Context){
	var (
		err          error
		rp           *model.Reply
		ats          []int64
		root, parent int64
	)

	params := c.Request.Form
	midStr, _ := c.Get("mid")
	mid := midStr.(int64)
	oidStr := params.Get("oid")
	tpStr := params.Get("type")
	rtStr := params.Get("root")
	paStr := params.Get("parent")
	atStr := params.Get("at")
	oid, err := strconv.ParseInt(oidStr, 10, 64)
	if err != nil || oid <= 0 {
		log.Warn("strconv.ParseInt(%s) error(%v)", oidStr, err)
		err = ecode.RequestErr
		c.JSON(nil, err)
		return
	}
	if rtStr != "" {
		root, err = strconv.ParseInt(rtStr, 10, 64)
		if err != nil {
			log.Warn("strconv.ParseInt(%s) error(%v)", rtStr, err)
			err = ecode.RequestErr
			c.JSON(nil, err)
			return
		}
	}
	if paStr != "" {
		parent, err = strconv.ParseInt(paStr, 10, 64)
		if err != nil {
			log.Warn("strconv.ParseInt(%s) error(%v)", paStr, err)
			err = ecode.RequestErr
			c.JSON(nil, err)
			return
		}
	}
	if !((root == 0 && parent == 0) || (root > 0 && parent > 0)) {
		log.Warn("the wrong root(%d) and parent(%d)", root, parent)
		err = ecode.RequestErr
		c.JSON(nil, err)
		return
	}
	tp, err := strconv.ParseInt(tpStr, 10, 8)
	if err != nil {
		log.Warn("strconv.ParseInt(%s) error(%v)", tpStr, err)
		err = ecode.RequestErr
		c.JSON(nil, err)
		return
	}
	if !model.LegalSubjectType(int8(tp)) {
		err = ecode.ReplyIllegalSubType
		c.JSON(nil, err)
		return
	}
	if atStr != "" {
		ats, err = xstr.SplitInts(atStr)
		if err != nil {
			log.Warn("utils.SplitInts(%s) error(%v)", atStr, err)
			err = ecode.RequestErr
			c.JSON(nil, err)
			return
		}
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
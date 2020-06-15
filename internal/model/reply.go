package model

import (
	xtime "github.com/go-kratos/kratos/pkg/time"
)

// subtype
const (
	SubStateNormal = int8(0)
	SubStateForbid = int8(1)

	SortByFloor = int8(0)
	SortByCount = int8(1)
	SortByLike  = int8(2)

	ReplyStateNormal    = int8(0)  // normal
	ReplyStateHidden    = int8(1)  // hidden by up
	ReplyStateFiltered  = int8(2)  // filtered
	ReplyStateAdminDel  = int8(3)  // delete by admin
	ReplyStateUserDel   = int8(4)  // delete by user
	ReplyStateMonitor   = int8(5)  // reply after audit
	ReplyStateGarbage   = int8(6)  // spam reply
	ReplyStateTop       = int8(7)  // top
	ReplyStateUpDel     = int8(8)  // delete by up
	ReplyStateBlacklist = int8(9)  // in a blacklist
	ReplyStateAssistDel = int8(10) // delete by assistant
	ReplyStateAudit     = int8(11) // 监管中
	ReplyStateFolded    = int8(12) // 被折叠
)

// Reply Reply
type Reply struct {
	RpID      int64      `json:"rpid"`
	Oid       int64      `json:"oid"`
	Type      int8       `json:"type"`
	Mid       int64      `json:"mid"`
	Root      int64      `json:"root"`
	Parent    int64      `json:"parent"`
	Dialog    int64      `json:"dialog"`
	Count     int        `json:"count"`
	RCount    int        `json:"rcount"`
	Floor     int        `json:"floor,omitempty"`
	State     int8       `json:"state"`
	FansGrade int8       `json:"fansgrade"`
	Attr      uint32     `json:"attr"`
	CTime     xtime.Time `json:"ctime"`
	MTime     xtime.Time `json:"-"`
	// string
	RpIDStr   string `json:"rpid_str,omitempty"`
	RootStr   string `json:"root_str,omitempty"`
	ParentStr string `json:"parent_str,omitempty"`
	DialogStr string `json:"dialog_str,omitempty"`
	// action count, from ReplyAction count
	Like   int  `json:"like"`
	Hate   int  `json:"-"`
	Action int8 `json:"action"`
	// member info
	//Member *Member `json:"member"`
	// other
	Content *Content `json:"content"`
	Replies []*Reply `json:"replies"`
	Assist  int      `json:"assist"`
	// 是否有折叠评论
	//Folder Folder `json:"folder"`
}

// Subject ReplySubject
type Subject struct {
	ID     int64      `json:"-"`
	Oid    int64      `json:"oid"`
	Type   int8       `json:"type"`
	Mid    int64      `json:"mid"`
	Count  int        `json:"count"`
	RCount int        `json:"rcount"`
	ACount int        `json:"acount"`
	State  int8       `json:"state"`
	Attr   uint32     `json:"attr"`
	Meta   string     `json:"meta"`
	CTime  xtime.Time `json:"ctime"`
	MTime  xtime.Time `json:"-"`
}

// Content ReplyContent
type Content struct {
	RpID    int64  `json:"-"`
	Message string `json:"message"`

	Ats Int64Bytes `json:"ats,omitempty"`
	//Topics  Mstr       `json:"topics,omitempty"`
	IP      uint32     `json:"ipi,omitempty"`
	Plat    int8       `json:"plat"`
	Device  string     `json:"device"`
	Version string     `json:"version,omitempty"`
	CTime   xtime.Time `json:"-"`
	MTime   xtime.Time `json:"-"`
	// ats member info
	//Members []*Info `json:"members"`
}

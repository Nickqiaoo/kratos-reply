package model

const (
	// AttrNo attribute no
	AttrNo = uint32(0)
	// AttrYes attribute yes
	AttrYes = uint32(1)
)

// PageParam reply page param.
type PageParam struct {
	Mid  int64 `form:"mid" validate:"min=1,required"`
	Oid  int64 `form:"oid" validate:"min=1,required"`
	Type int8  `form:"type" validate:"required"`
	Sort int8  `form:"sort" validate:"required"`

	Pn int `form:"pn" validate:"required"`
	Ps int `form:"ps" validate:"required"`

	NeedHot bool `form:"needhot" validate:"required"`
}

// PageResult reply page result.
type PageResult struct {
	Subject  *Subject
	TopAdmin *Reply
	TopUpper *Reply
	Roots    []*Reply
	Hots     []*Reply
	Total    int
	AllCount int
}

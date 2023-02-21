package proto

import "github.com/hashicorp/hcl/v2"

func (x *Pos) ToHcl() hcl.Pos {
	return hcl.Pos{
		Line:   int(x.Line),
		Column: int(x.Column),
		Byte:   int(x.Byte),
	}
}

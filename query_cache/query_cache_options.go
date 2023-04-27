package query_cache

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"time"
)

type QueryCacheOptions struct {
	Enabled   bool
	Ttl       time.Duration
	MaxSizeMb int
}

func NewQueryCacheOptions(req *proto.SetCacheOptionsRequest) *QueryCacheOptions {
	return &QueryCacheOptions{
		Enabled:   req.Enabled,
		Ttl:       time.Duration(req.Ttl) * time.Second,
		MaxSizeMb: int(req.MaxSizeMb),
	}
}

package rate_limiter

import (
	"fmt"
	"golang.org/x/time/rate"
	"strings"
)

/*
bring back multi limiter
release all but longest limiter


when matching linters, only match if ALL limiter tags are populated

limiter defs are by default additive
limiter def can specify to stop searching down tree for other limiters (i.e. obverride base limiters)
support specifying limiters in plugin config (somehow)

differantiate between column tags and static tags


*/

type Config struct {
	Limit     rate.Limit
	BurstSize int
	// the tags which identify this limiter instance
	// one limiter instance will be created for each combination of tags which is encountered
	Tags []string
}

// CombineConfigs creates a new Confg by combining the provided config,
// starting with the first and only using values from subsequent configs if they have not already been set
// This is to allow building of a config from a base coinfig, overriding specific values by child configs
func CombineConfigs(configs []*Config) *Config {
	res := &Config{}
	for _, c := range configs {
		if c == nil {
			// not expected
			continue
		}
		if res.Limit == 0 && c.Limit != 0 {
			c.Limit = res.Limit
		}
		if res.BurstSize == 0 && c.BurstSize != 0 {
			c.BurstSize = res.BurstSize
		}
		if len(res.Tags) == 0 && len(res.Tags) != 0 {
			c.Tags = res.Tags
		}
	}
	return res

}

func (c *Config) String() string {
	return fmt.Sprintf("Limit(/s): %v, Burst: %d, Tags: %s", c.Limit, c.BurstSize, strings.Join(c.Tags, ","))
}

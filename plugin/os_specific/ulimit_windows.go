// +build windows

package os_specific

import "github.com/hashicorp/go-hclog"

func SetRlimit(ulimit uint64, logger hclog.Logger) error {
	logger.Trace("Ignoring Set UNIX rlimit call", "ulimit", ulimit)
	return nil
}

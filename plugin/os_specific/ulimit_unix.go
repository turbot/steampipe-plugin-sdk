// +build !windows

package os_specific

import "github.com/hashicorp/go-hclog"

func SetRlimit(ulimit uint64, logger hclog.Logger ) error {
	var rLimit syscall.Rlimit
	rLimit.Max = ulimit
	rLimit.Cur = ulimit
	logger.Trace("Setting Ulimit", "ulimit", ulimit)
	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
}

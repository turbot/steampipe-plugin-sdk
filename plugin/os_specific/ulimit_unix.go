//go:build !windows
// +build !windows

package os_specific

import (
	"syscall"
)

func SetRlimit(ulimit uint64) error {
	var rLimit syscall.Rlimit
	rLimit.Max = ulimit
	rLimit.Cur = ulimit

	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
}

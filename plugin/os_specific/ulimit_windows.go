//go:build windows
// +build windows

package os_specific

func SetRlimit(ulimit uint64) error {
	return nil
}

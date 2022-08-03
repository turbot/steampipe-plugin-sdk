package mem

import (
	sigar "github.com/cloudfoundry/gosigar"
	"log"
	"os"
)

func GetProcessMemory() float64 {
	mem := sigar.ProcMem{}
	pid := os.Getpid()
	if err := mem.Get(pid); err != nil {
		log.Println(err)
	}
	return float64(mem.Resident) / (1024 * 1024)

}

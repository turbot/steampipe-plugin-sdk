package logging

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
)

type timeLog struct {
	Time       time.Time
	Interval   time.Duration
	Cumulative time.Duration
	Operation  string
}

var timing []timeLog

func shouldProfile() bool {
	return strings.ToUpper(os.Getenv(ProfileEnvVar)) == "TRUE" ||
		strings.ToUpper(os.Getenv(LegacyProfileEnvVar)) == "TRUE"
}

func LogTime(operation string) {
	if !shouldProfile() {
		return
	}
	lastTimelogIdx := len(timing) - 1
	var elapsed time.Duration
	var cumulative time.Duration
	if lastTimelogIdx >= 0 {
		cumulative = time.Since(timing[0].Time)
		elapsed = time.Since(timing[lastTimelogIdx].Time)
	}
	timing = append(timing, timeLog{time.Now(), elapsed, cumulative, operation})
}

func ClearProfileData() {
	timing = []timeLog{}
}

func DisplayProfileData(minTime time.Duration) {
	if shouldProfile() {

		minString := fmt.Sprintf("< %s", minTime.String())
		var data [][]string
		for _, logEntry := range timing {
			var itemData []string
			itemData = append(itemData, logEntry.Operation)

			timeStr := fmt.Sprintf("%s", logEntry.Time.Format(time.StampMilli))
			itemData = append(itemData, timeStr)

			intervalStr := fmt.Sprintf("%s", logEntry.Interval)
			if logEntry.Interval < minTime {
				intervalStr = minString
			}
			itemData = append(itemData, intervalStr)

			cumulativeStr := fmt.Sprintf("%s", logEntry.Cumulative)
			if logEntry.Cumulative < minTime {
				cumulativeStr = minString
			}
			itemData = append(itemData, cumulativeStr)

			data = append(data, itemData)
		}

		var b bytes.Buffer
		writer := bufio.NewWriter(&b)
		displayTable(writer, data)
		_ = writer.Flush()

		// weird hoops we have to jump through to get this printing vial hclog
		s := ""
		for _, b := range b.Bytes() {
			if b == '\n' {
				log.Printf("[WARN] %s\n", s)
				s = ""
			} else {
				s += string(b)
			}
		}

		ClearProfileData()
	}

}

func displayTable(out io.Writer, data [][]string) {
	table := tablewriter.NewWriter(out)
	table.SetHeader([]string{"Operation", "Time", "Elapsed", "Cumulative"})
	table.SetBorder(true)
	table.SetColWidth(50)
	table.AppendBulk(data)
	table.SetAutoWrapText(false)
	table.Render()
}

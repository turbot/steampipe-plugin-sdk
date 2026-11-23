package logging

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

type timeLog struct {
	Time       time.Time
	Interval   time.Duration
	Cumulative time.Duration
	Operation  string
}

var timing []timeLog

func shouldProfile() bool {
	return strings.ToUpper(os.Getenv(EnvProfile)) == "TRUE"
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

			timeStr := logEntry.Time.Format(time.StampMilli)
			itemData = append(itemData, timeStr)

			intervalStr := logEntry.Interval.String()
			if logEntry.Interval < minTime {
				intervalStr = minString
			}
			itemData = append(itemData, intervalStr)

			cumulativeStr := logEntry.Cumulative.String()
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

var (
	timingHeader = []string{"OPERATION", "TIME", "ELAPSED", "CUMULATIVE"}
	// a cell that is only a number is right-aligned
	bareNumber = regexp.MustCompile(`^-?(?:\d{1,3}(?:,\d{3})*|\d+)(?:\.\d+)?$`)
)

// displayTable writes the rows as the bordered table that tablewriter v0.0.5 printed
func displayTable(out io.Writer, data [][]string) {
	widths := make([]int, len(timingHeader))
	for c, h := range timingHeader {
		widths[c] = len(h)
	}
	cells := make([][][]string, len(data))
	for r, row := range data {
		cells[r] = make([][]string, len(timingHeader))
		for c := range timingHeader {
			var width int
			if c < len(row) {
				cells[r][c], width = cellLines(row[c])
			}
			widths[c] = max(widths[c], width)
		}
	}

	var b strings.Builder
	border := func() {
		for _, w := range widths {
			b.WriteString("+" + strings.Repeat("-", w+2))
		}
		b.WriteString("+\n")
	}
	border()
	for c, h := range timingHeader {
		gap := widths[c] - len(h)
		b.WriteString("| " + strings.Repeat(" ", gap/2) + h + strings.Repeat(" ", gap-gap/2) + " ")
	}
	b.WriteString("|\n")
	border()
	for _, row := range cells {
		height := 1
		for _, lines := range row {
			height = max(height, len(lines))
		}
		for i := 0; i < height; i++ {
			for c, lines := range row {
				line := ""
				if i < len(lines) {
					line = lines[i]
				}
				pad := strings.Repeat(" ", widths[c]-utf8.RuneCountInString(line))
				if bareNumber.MatchString(strings.TrimSpace(line)) {
					b.WriteString("| " + pad + line + " ")
				} else {
					b.WriteString("| " + line + pad + " ")
				}
			}
			b.WriteString("|\n")
		}
	}
	border()
	_, _ = io.WriteString(out, b.String())
}

// cellLines returns the lines a cell prints as and the width it needs, as tablewriter v0.0.5 did:
// the text's lines are joined and wrapped again at the longest one's width, at most 50, and a
// word longer than that widens the cell
func cellLines(cell string) ([]string, int) {
	limit := 0
	for _, line := range strings.Split(cell, "\n") {
		limit = max(limit, utf8.RuneCountInString(line))
	}
	limit = min(limit, 50)
	lines := wrapWords(strings.ReplaceAll(cell, "\n", " "), limit)
	width := limit
	for _, line := range lines {
		width = max(width, utf8.RuneCountInString(line))
	}
	return lines, width
}

// wrapWords splits s into lines of at most lim characters, never breaking a word, at the breaks
// that leave the least ragged edge
func wrapWords(s string, lim int) []string {
	words := strings.Split(s, " ")
	n := len(words)
	// start[i] is the width of the words before word i, with one space after each
	start := make([]int, n+1)
	for i, w := range words {
		lim = max(lim, utf8.RuneCountInString(w))
		start[i+1] = start[i] + utf8.RuneCountInString(w) + 1
	}
	lineWidth := func(i, j int) int { return start[j+1] - start[i] - 1 }

	const penalty = 1e5
	next := make([]int, n)
	cost := make([]int, n)
	for i := n - 1; i >= 0; i-- {
		if lineWidth(i, n-1) <= lim {
			next[i] = n
			continue
		}
		cost[i] = math.MaxInt32
		for j := i + 1; j < n; j++ {
			d := lim - lineWidth(i, j-1)
			c := d*d + cost[j]
			if lineWidth(i, j-1) > lim {
				c += penalty
			}
			if c < cost[i] {
				cost[i] = c
				next[i] = j
			}
		}
	}
	var lines []string
	for i := 0; i < n; i = next[i] {
		lines = append(lines, strings.Join(words[i:next[i]], " "))
	}
	return lines
}

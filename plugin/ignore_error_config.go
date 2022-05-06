package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

type IgnoreConfig struct {
	ShouldIgnoreError     ErrorPredicate
	ShouldIgnoreErrorFunc ErrorPredicateWithContext
}

func (c *IgnoreConfig) String() interface{} {
	var s strings.Builder
	if c.ShouldIgnoreError != nil {
		s.WriteString(fmt.Sprintf("ShouldIgnoreError: %s\n", helpers.GetFunctionName(c.ShouldIgnoreError)))
	}
	if c.ShouldIgnoreErrorFunc != nil {
		s.WriteString(fmt.Sprintf("ShouldIgnoreErrorFunc: %s\n", helpers.GetFunctionName(c.ShouldIgnoreErrorFunc)))
	}
	return s.String()
}

func (c *IgnoreConfig) Validate(table *Table) []string {
	log.Printf("[TRACE] IgnoreConfig validate: ShouldIgnoreError %p, ShouldIgnoreErrorFunc: %p", c.ShouldIgnoreError, c.ShouldIgnoreErrorFunc)

	if c.ShouldIgnoreError != nil && c.ShouldIgnoreErrorFunc != nil {
		log.Printf("[TRACE] IgnoreConfig validate failed - both ShouldIgnoreError and ShouldIgnoreErrorFunc are defined")
		return []string{fmt.Sprintf("table '%s' both ShouldIgnoreError and ShouldIgnoreErrorFunc are defined", table.Name)}
	}
	return nil
}

func (c *IgnoreConfig) DefaultTo(other *IgnoreConfig) {
	// if either ShouldIgnoreError or ShouldIgnoreErrorFunc are set, do not default to other
	if c.ShouldIgnoreError != nil || c.ShouldIgnoreErrorFunc != nil {
		log.Printf("[TRACE] IgnoreConfig DefaultTo: config defines a should ignore function so not defaulting to base")
		return
	}

	// legacy func
	if c.ShouldIgnoreError == nil && other.ShouldIgnoreError != nil {
		log.Printf("[TRACE] IgnoreConfig DefaultTo: using base ShouldIgnoreError: %s", helpers.GetFunctionName(other.ShouldIgnoreError))
		c.ShouldIgnoreError = other.ShouldIgnoreError
	}
	if c.ShouldIgnoreErrorFunc == nil && other.ShouldIgnoreErrorFunc != nil {
		log.Printf("[TRACE] IgnoreConfig DefaultTo: using base ShouldIgnoreErrorFunc: %s", helpers.GetFunctionName(other.ShouldIgnoreErrorFunc))
		c.ShouldIgnoreErrorFunc = other.ShouldIgnoreErrorFunc
	}
}

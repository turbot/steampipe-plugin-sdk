package plugin

import (
	"sync"

	"github.com/gertd/go-pluralize"
)

var pluralizeClient = sync.OnceValue(pluralize.NewClient)

// Package plugin provides data structures and functions that enable a plugin to read data from an API
// and stream it into Postgres tables by way of Steampipe's [foreign data wrapper] (FDW).
package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v4/connection"
)

type ForceImport string

var forceImportConnection connection.ForceImport

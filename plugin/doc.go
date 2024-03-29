// Package plugin provides data structures and functions that enable a plugin to read data from an API
// and stream it into Postgres tables by way of Steampipe's [foreign data wrapper] (FDW).
package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/connection"
	"github.com/turbot/steampipe-plugin-sdk/v5/docs/dynamic_tables"
	"github.com/turbot/steampipe-plugin-sdk/v5/docs/error_handling"
	"github.com/turbot/steampipe-plugin-sdk/v5/docs/key_columns"
	"github.com/turbot/steampipe-plugin-sdk/v5/docs/matrix_items"
)

// ForceImport is a mechanism to ensure godoc can reference all required packages
type ForceImport string

var forceImportConnection connection.ForceImport
var forceImportDynamicPlugin dynamic_tables.ForceImport
var forceImportKeyColumns key_columns.ForceImport
var forceImportErrorHandling error_handling.ForceImport
var forceImportMatrixItems matrix_items.ForceImport

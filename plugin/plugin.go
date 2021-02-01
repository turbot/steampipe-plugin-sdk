package plugin

import (
	"context"
	"fmt"
	"log"

	"github.com/turbot/steampipe-plugin-sdk/grpc"

	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
)

// Plugin :: an object used to build all necessary data for a given query
type Plugin struct {
	Name             string
	Logger           hclog.Logger
	TableMap         map[string]*Table
	DefaultTransform *transform.ColumnTransforms
	DefaultGetConfig *GetConfig
	DefaultHydrateConfig *DefaultHydrateConfig
	// every table must implement these columns
	RequiredColumns []*Column
}

func columnTypeToString(columnType proto.ColumnType) string {
	switch columnType {
	case proto.ColumnType_BOOL:
		return "ColumnType_BOOL"
	case proto.ColumnType_INT:
		return "ColumnType_INT"
	case proto.ColumnType_DOUBLE:
		return "ColumnType_DOUBLE"
	case proto.ColumnType_STRING:
		return "ColumnType_STRING"
	case proto.ColumnType_JSON:
		return "ColumnType_BOOL"
	case proto.ColumnType_DATETIME:
		return "ColumnType_DATETIME"
	case proto.ColumnType_IPADDR:
		return "ColumnType_IPADDR"
	case proto.ColumnType_CIDR:
		return "ColumnType_CIDR"
	default:
		return fmt.Sprintf("Unknown column type: %v", columnType)
	}
}

func (p *Plugin) GetSchema() (map[string]*proto.TableSchema, error) {
	// first validate the plugin - this will panic with a validation error
	if validationErrors := p.Validate(); validationErrors != "" {
		return nil, fmt.Errorf("plugin %s validation failed: \n%s", p.Name, validationErrors)
	}

	schema := map[string]*proto.TableSchema{}

	for tableName, table := range p.TableMap {
		schema[tableName] = table.GetSchema()
	}
	return schema, nil
}

// Execute :: execute a query and stream the results
func (p *Plugin) Execute(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()

	logging.LogTime("Start execute")

	queryContext := req.QueryContext
	table, ok := p.TableMap[req.Table]
	if !ok {
		return fmt.Errorf("plugin %s does not provide table %s", p.Name, req.Table)
	}

	log.Printf("[TRACE] Execute: cols: %v\n", queryContext.Columns)
	log.Printf("[TRACE] quals: %s\n", grpc.QualMapToString(queryContext.Quals))

	// async approach
	// 1) call list() in a goroutine. This writes pages of items to the rowDataChan. When complete it closes the channel
	// 2) range over rowDataChan - for each item spawn a goroutine to build a row
	// 3) Build row spawns goroutines for any required hydrate functions.
	// 4) When hydrate functions are complete, apply transforms to generate column values. When row is ready, send on rowChan
	// 5) Range over rowChan - for each row, send on results stream

	d := newQueryData(queryContext, table, stream)
	ctx := context.WithValue(context.Background(), ContextKeyLogger, p.Logger)
	log.Printf("[TRACE] calling fetchItems, table: %s\n", table.Name)

	// asyncronously fetch items
	table.fetchItems(ctx, d)

	log.Println("[TRACE] after fetchItems")

	logging.LogTime("Calling stream")

	// asyncronously build rows
	rowChan := d.buildRows(ctx)
	// asyncronously stream rows
	return d.streamRows(ctx, rowChan)
}

// slightly hacky - called on startup to set a plugin pointer in each table
func (p *Plugin) claimTables() {
	for _, table := range p.TableMap {
		table.Plugin = p
	}
}

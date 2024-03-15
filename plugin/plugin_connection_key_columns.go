package plugin

import (
	"context"
	typeHelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/quals"
	"golang.org/x/exp/maps"
)

func (p *Plugin) addConnectionKeyColumns(tableName string, t *proto.TableSchema) {
	// if this column is defined by the plugin as a connection key column (or is the column 'sp_connection_name')
	// add as a key column
	for _, c := range t.Columns {
		if p.isConnectionKeyColumn(c.Name) {
			p.addKeyColumn(c, t)
		}
	}
}

// if this column is defined by the plugin as a connection key column (or is the 'column sp_connection_name')
// add key column it
func (p *Plugin) addKeyColumn(column *proto.ColumnDefinition, tableSchema *proto.TableSchema) {
	// add to the get and list key columns
	kc := &proto.KeyColumn{
		Name:      column.Name,
		Operators: []string{quals.QualOperatorEqual, quals.QualOperatorNotEqual, quals.QualOperatorLike, quals.QualOperatorILike, quals.QualOperatorNotLike, quals.QualOperatorNotILike},
		Require:   Optional,
	}
	// check whether we already have a key column for this column
	getKeyColumns := tableSchema.GetKeyColumnMap()
	if _, alreadyHasGetKeyColumn := getKeyColumns[column.Name]; !alreadyHasGetKeyColumn {
		// no get key column - add one
		tableSchema.GetCallKeyColumnList = append(tableSchema.GetCallKeyColumnList, kc)
	}
	// check whether we already have a list key column for this column
	listKeyColumns := tableSchema.ListKeyColumnMap()
	if _, alreadyHasListKeyColumn := listKeyColumns[column.Name]; !alreadyHasListKeyColumn {
		// no list key column - add one
		tableSchema.ListCallKeyColumnList = append(tableSchema.ListCallKeyColumnList, kc)
	}
}

// filterConnectionsWithKeyColumns filters the list of connections
// by applying any connection key column quals included in qualMap
func (p *Plugin) filterConnectionsWithKeyColumns(ctx context.Context, connectionData map[string]*proto.ExecuteConnectionData, qualMap map[string]*proto.Quals) map[string]*proto.ExecuteConnectionData {
	// shallow clone the connection data to return
	var res = maps.Clone(connectionData)

	// if this plugin does not support connectionKeyColumns, nothing to do
	if len(p.connectionKeyColumnsMap) == 0 {
		return connectionData
	}

	// if any connectionKeyColumnQuals were provided, ONLY return the connections which match the quals
	for column, qualList := range qualMap {
		if !p.isConnectionKeyColumn(column) {
			continue
		}

		// so this IS a connection key column

		// iterate over the requested  connections and remove any which do not match the quals
		for connectionName := range connectionData {
			// get the column value for this connection (this lazily loads the value into connectionKeyColumnValuesMap)
			columnValue, err := p.getConnectionKeyColumnValue(ctx, connectionName, column)
			if err != nil {
				// if the function fails,
				// if we cannot get the column value, remove the connection
				delete(res, connectionName)
				continue
			}

			var includeConnection = true

			// not sure if in practice we would get multiple qualList for a column but the data structure supports it
			for _, qual := range qualList.Quals {
				v := []*proto.QualValue{qual.Value}
				// handle 'in' (in which case the value will be a list)
				if listValue := qual.Value.GetListValue(); listValue != nil {
					v = listValue.Values
				}

				switch qual.Operator.(*proto.Qual_StringValue).StringValue {
				case quals.QualOperatorEqual:
					includeConnection = qualValuesContainValue(v, columnValue)
				case quals.QualOperatorNotEqual:
					includeConnection = !qualValuesContainValue(v, columnValue)
				case quals.QualOperatorLike:
					includeConnection = SqlLike(typeHelpers.ToString(columnValue), qual.Value.GetStringValue(), true)
				case quals.QualOperatorILike:
					includeConnection = SqlLike(typeHelpers.ToString(columnValue), qual.Value.GetStringValue(), false)
				case quals.QualOperatorNotLike:
					includeConnection = !SqlLike(typeHelpers.ToString(columnValue), qual.Value.GetStringValue(), true)
				case quals.QualOperatorNotILike:
					includeConnection = !SqlLike(typeHelpers.ToString(columnValue), qual.Value.GetStringValue(), false)

				default:
					// unsupported operator ignore
				}
			}
			// if we are NOT including the connection, delete from the map
			if !includeConnection {
				delete(res, connectionName)
			}
		}
	}

	return res
}

// do any of the given qual values contain 'value'
func qualValuesContainValue(qualValues []*proto.QualValue, value any) bool {
	for _, v := range qualValues {
		v := grpc.GetQualValue(v)
		if value == v {
			return true
		}
	}
	return false

}

// clears the values of connectionKeyColumnValuesMap for the given connections
// the is called when we set connection config - used to force a new (lazy) load of the values
func (p *Plugin) clearConnectionKeyColumnValues(configs []*proto.ConnectionConfig) {
	p.connectionKeyColumnValuesMapLock.Lock()
	defer p.connectionKeyColumnValuesMapLock.Unlock()
	for _, c := range configs {
		delete(p.connectionKeyColumnValuesMap, c.Connection)
	}
}

func (p *Plugin) getConnectionKeyColumnValue(ctx context.Context, connectionName string, column string) (any, error) {
	// special case for `sp_connection_name` - just return the connection name
	if column == connectionNameColumnName {
		return connectionName, nil
	}

	// lock the map
	p.connectionKeyColumnValuesMapLock.Lock()
	// ensure we unlock it
	defer p.connectionKeyColumnValuesMapLock.Unlock()

	// get the column value map for this connection
	columnValueMap, ok := p.connectionKeyColumnValuesMap[connectionName]
	// create if needed
	if !ok {
		columnValueMap = make(map[string]any)
		p.connectionKeyColumnValuesMap[connectionName] = columnValueMap
	}
	columnValue, ok := columnValueMap[column]

	// if we already have the value, return it
	if ok {
		return columnValue, nil
	}

	// we do not yet have the value stored - call the function to get it
	valueFunc := p.connectionKeyColumnsMap[column].Hydrate
	connectionCache, err := p.ensureConnectionCache(connectionName)
	if err != nil {
		return nil, err
	}
	d := &QueryData{
		Connection:      p.ConnectionMap[connectionName].Connection,
		ConnectionCache: connectionCache,
	}
	h := &HydrateData{}
	val, err := valueFunc(ctx, d, h)
	if err != nil {
		return nil, err
	}
	// store the value
	columnValueMap[column] = val
	return val, nil
}

func (p *Plugin) isConnectionKeyColumn(column string) bool {
	// special case for `sp_connection_name`
	if column == connectionNameColumnName {
		return true
	}

	_, isConnectionKeyColumn := p.connectionKeyColumnsMap[column]
	return isConnectionKeyColumn
}

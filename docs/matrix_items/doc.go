/*
Matrix Items

Matrix items are a powerful way of executing the same query multiple times in parallel for a set of parameters, for example regions.

Each plugin table can optionally define a callback function [plugin.Table.GetMatrixItem].

This returns a list of maps, each of which contains the parameters required to do get/list
for a given region (or whatever partitioning is relevant to the plugin)

The plugin would typically get this information from the connection config.
If a matrix is returned by the plugin, we execute Get/List calls for each matrix item (e.g. each region).

NOTE: if the quals include the matrix property (or properties),
we check whether each matrix item meets the quals and if not, do not execute for that item

For example, for the query:

	select vpc_id, region from aws_vpc where region = 'us-east-1'

we would only execute a List function for the matrix item { region: "us-east-1" },
even if other were defined in the connection config

When executing for each matrix item, the matrix item is put into the context, available for use by the Get/List/Hydrate call
*/
package matrix_items

// ForceImport is a mechanism to ensure godoc can reference all required packages
type ForceImport string

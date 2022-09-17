/* Package matrix_items
# Matrix Items
[matrix_items] are a powerful way of executing the same query multiple times in parallel for a set of parameters, for example regions.

It is cumbersome to define different set of regions every time a hydrate function is invoked. MatrixItemMapFunc helps you to define a set of regions which can execute the API calls parallely and then unify the results into different rows.
In certain cloud providers, region data needs to be passed into the [HydrateFunc] for execution. If we define...
TODO
*/

package matrix_items

// ForceImport is a mechanism to ensure godoc can reference all required packages
type ForceImport string

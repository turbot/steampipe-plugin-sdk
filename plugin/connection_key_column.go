package plugin

type ConnectionKeyColumn struct {
	Hydrate HydrateFunc
	name    string
}

func (c *ConnectionKeyColumn) initialise(columnName string) {
	c.name = columnName
}

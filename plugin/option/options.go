package option

type KeyColumnSetOptions struct {
	MinimumQuals int
}

func WithAtLeast(minimumQuals int) KeyColumnSetOptions {
	return KeyColumnSetOptions{MinimumQuals: minimumQuals}
}

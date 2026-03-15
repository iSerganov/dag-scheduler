package dag

import "context"

// funcTask adapts a plain function into a Task.
type funcTask struct {
	id   uint64
	name string
	fn   func(context.Context) error
}

func (f *funcTask) ID() uint64                    { return f.id }
func (f *funcTask) Name() string                  { return f.name }
func (f *funcTask) Run(ctx context.Context) error { return f.fn(ctx) }

// Func creates a Task from a plain function, id, and name.
// It is a convenience constructor for callers that do not need a custom type.
func Func(id uint64, name string, fn func(context.Context) error) Task {
	return &funcTask{id: id, name: name, fn: fn}
}

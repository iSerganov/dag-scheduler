package dag

import "context"

// FuncTask adapts a plain function into a Task.
type FuncTask struct {
	id   uint64
	name string
	fn   func(context.Context) error
}

// ID returns the ID of the task.
func (f *FuncTask) ID() uint64 { return f.id }

// Name returns the name of the task.
func (f *FuncTask) Name() string { return f.name }

// Run runs the task.
func (f *FuncTask) Run(ctx context.Context) error { return f.fn(ctx) }

// Func creates a Task from a plain function, id, and name.
// It is a convenience constructor for callers that do not need a custom type.
func Func(id uint64, name string, fn func(context.Context) error) *FuncTask {
	return &FuncTask{id: id, name: name, fn: fn}
}

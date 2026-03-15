package dag

import "context"

// Task is a function that can be executed by the scheduler.
// It must implement the Run method to perform the actual work.
// It must also implement the ID method to return a unique identifier for the task.
type Task interface {
	Run(ctx context.Context) error
	ID() uint64
	Name() string
}

// Node is a node in the DAG.
// It contains the task to be executed and the dependencies of the task.
type Node struct {
	Deps []Task
	Task Task
}

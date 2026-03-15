# dag-scheduler

A Go library for building and executing Directed Acyclic Graphs (DAGs) of tasks.
Independent tasks run concurrently the moment all their dependencies finish,
giving maximum parallelism with no thread-pool configuration required.
A step-by-step execution mode is also available for controlled, one-task-at-a-time
traversal.

## Table of contents

- [dag-scheduler](#dag-scheduler)
	- [Table of contents](#table-of-contents)
	- [Repository layout](#repository-layout)
	- [Core concepts](#core-concepts)
	- [Quick start](#quick-start)
	- [The dag package](#the-dag-package)
		- [Task interface](#task-interface)
		- [dag.Func convenience constructor](#dagfunc-convenience-constructor)
		- [Node](#node)
		- [DAG](#dag)
		- [Topological sort](#topological-sort)
		- [Sentinel errors (dag)](#sentinel-errors-dag)
	- [The scheduler package](#the-scheduler-package)
		- [Creating a scheduler](#creating-a-scheduler)
		- [Registering tasks](#registering-tasks)
		- [Full parallel execution: Run](#full-parallel-execution-run)
		- [Step-by-step execution: RunNext](#step-by-step-execution-runnext)
		- [Inspecting the execution plan](#inspecting-the-execution-plan)
		- [Sentinel errors (scheduler)](#sentinel-errors-scheduler)
	- [Implementing a custom Task](#implementing-a-custom-task)
	- [Error handling](#error-handling)
	- [Concurrency guarantees](#concurrency-guarantees)
	- [Running the example](#running-the-example)
	- [Running the tests with the race detector:](#running-the-tests-with-the-race-detector)

---

## Repository layout

```
dag-scheduler/
  dag/                  Core DAG data structure and Task interface.
    node.go             Task interface and Node struct.
    dag.go              Thread-safe DAG with AddNode, Dependents, InDegrees.
    topo.go             Kahn's algorithm topological sort (Sort method on DAG).
    func.go             dag.Func convenience constructor.
  scheduler/            Public scheduler API built on top of dag.
    scheduler.go        Scheduler, Run, RunNext, ExecutionPlan.
  examples/
    basic/              Runnable seven-task three-stage CI/CD pipeline example.
```

## Core concepts

A **Task** is any value that implements the `dag.Task` interface: it has a
unique numeric ID, a human-readable name, and a `Run` method that performs the
actual work.

A **Node** wraps a Task together with the list of Tasks it directly depends on.

A **DAG** is a directed acyclic graph of Nodes.  Edges point from dependency to
dependent.  Adding a node automatically wires the edges.

A **Scheduler** sits on top of a DAG and drives execution.  It supports two
execution modes:

- `Run` -- full concurrent execution with maximum parallelism.
- `RunNext` -- deterministic, one-task-at-a-time stepping through the DAG.

---

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/iSerganov/dag-scheduler/dag"
    "github.com/iSerganov/dag-scheduler/scheduler"
)

func main() {
    fetch := dag.Func(1, "fetch", func(_ context.Context) error {
        fmt.Println("fetching inputs")
        return nil
    })
    build := dag.Func(2, "build", func(_ context.Context) error {
        fmt.Println("building artifacts")
        return nil
    })
    deploy := dag.Func(3, "deploy", func(_ context.Context) error {
        fmt.Println("deploying release")
        return nil
    })

    s := scheduler.New()
    if err := s.AddTask(fetch); err != nil {
        log.Fatal(err)
    }
    if err := s.AddTask(build, fetch); err != nil {
        log.Fatal(err)
    }
    if err := s.AddTask(deploy, build); err != nil {
        log.Fatal(err)
    }

    if err := s.Run(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

Output (tasks with no dependency relationship may appear in any order):

```
fetching inputs
building artifacts
deploying release
```

---

## The dag package

### Task interface

```go
type Task interface {
    ID()   uint64
    Name() string
    Run(context.Context) error
}
```

- `ID` must return a value that is unique across every Task registered in the
  same Scheduler.  IDs are used as map keys internally; collisions result in an
  `ErrNodeExists` error at registration time.
- `Name` is a human-readable label used in error messages and the execution
  plan.  It does not need to be unique.
- `Run` performs the task's work.  A non-nil return value is treated as a
  failure and propagates through the scheduler.

### dag.Func convenience constructor

For tasks that do not require a custom struct, `dag.Func` wraps a plain
function:

```go
func Func(id uint64, name string, fn func(context.Context) error) Task
```

Example:

```go
t := dag.Func(42, "compress", func(ctx context.Context) error {
    return compress(ctx, "output.tar.gz")
})
```

### Node

```go
type Node struct {
    Task Task
    Deps []Task
}
```

A `Node` pairs a `Task` with its direct dependencies.  When `Deps` is nil or
empty the task is a root node (no prerequisites).  Nodes are registered with
`DAG.AddNode`; the DAG wires the edges automatically from the `Deps` slice.

### DAG

`dag.DAG` is a thread-safe directed acyclic graph.

```go
func New() *DAG
func (d *DAG) AddNode(n *Node) error
func (d *DAG) Node(id uint64) (*Node, bool)
func (d *DAG) Nodes() []*Node
func (d *DAG) Dependents(id uint64) []*Node
func (d *DAG) InDegrees() map[uint64]int
func (d *DAG) Len() int
```

**AddNode** registers `n` and records an outgoing edge from every Task in
`n.Deps` to `n.Task`.  All dependencies must already be present.

```go
d := dag.New()

a := dag.Func(1, "a", run)
b := dag.Func(2, "b", run)

_ = d.AddNode(&dag.Node{Task: a})
_ = d.AddNode(&dag.Node{Task: b, Deps: []dag.Task{a}})
```

**Dependents** returns the nodes that directly depend on a given node (i.e. the
nodes that will become runnable once the given node completes).

**InDegrees** returns a copy of the current in-degree map -- the number of
unresolved dependencies for each node.  Useful for custom scheduling logic.

**Len** returns the number of registered nodes.

### Topological sort

```go
func (d *DAG) Sort() ([]*Node, error)
```

`Sort` implements Kahn's algorithm (O(V + E)) and returns the nodes in a valid
topological order.  It returns `dag.ErrCycle` if the graph contains a cycle.

### Sentinel errors (dag)

| Error | Condition |
|---|---|
| `dag.ErrNodeExists` | A node with the same Task ID is already registered. |
| `dag.ErrNodeNotFound` | A dependency referenced in `Node.Deps` has not been added yet. |
| `dag.ErrCycle` | The graph is not acyclic (returned by `Sort`). |

All errors wrap the sentinel so they are detectable with `errors.Is`.

---

## The scheduler package

### Creating a scheduler

```go
s := scheduler.New()
```

### Registering tasks

```go
func (s *Scheduler) AddTask(t dag.Task, deps ...dag.Task) error
```

`AddTask` registers `t` and wires it to all listed dependencies.  Every
dependency must have been registered before the task that depends on it.

```go
download := dag.Func(1, "download", ...)
validate := dag.Func(2, "validate", ...)
process  := dag.Func(3, "process",  ...)

_ = s.AddTask(download)
_ = s.AddTask(validate, download)
_ = s.AddTask(process,  validate, download) // two dependencies
```

Registering a task with a duplicate ID or an unregistered dependency returns an
error.  The Scheduler remains fully usable after such an error -- no partial
state is written.

Calling `AddTask` after `RunNext` has been used resets the step-execution state,
so the next `RunNext` call traverses the updated graph from the beginning.

### Full parallel execution: Run

```go
func (s *Scheduler) Run(ctx context.Context) error
```

`Run` validates the DAG (cycle detection), then starts every root task as a
separate goroutine.  When a task finishes it atomically decrements the
in-degree counter of each dependent; any dependent whose counter reaches zero
is launched immediately.  This gives true maximum parallelism -- a task starts
the instant all of its dependencies are done, with no batching or wave
synchronisation.

**Error and cancellation behaviour**

- If any task returns a non-nil error, the first such error is captured and
  returned after the graph drains.  Subsequent errors are discarded.
- When a task fails or `ctx` is cancelled, all tasks that depend on the failed
  or cancelled task are skipped (their `Run` method is never called), but the
  WaitGroup is still decremented for every skipped node so `Run` always
  terminates.
- `Run` returns `ErrTaskInProgress` immediately if `RunNext` is concurrently
  executing a task.

### Step-by-step execution: RunNext

```go
func (s *Scheduler) RunNext(ctx context.Context) error
```

`RunNext` executes exactly one ready task per call, in topological order, and
returns.  Repeated calls step through the entire DAG:

```go
for {
    err := s.RunNext(ctx)
    if errors.Is(err, scheduler.ErrExhausted) {
        break // all tasks have been executed
    }
    if err != nil {
        log.Printf("task failed: %v", err)
        // decide whether to continue or abort
    }
}
```

**State management**

Step state is initialised lazily on the first `RunNext` call and is shared
across calls, so the scheduler remembers which tasks have already been executed.
Calling `AddTask` invalidates the state; the next `RunNext` call reinitialises
it against the updated graph.

**Error behaviour**

When a task fails, `RunNext` returns the error immediately but still enqueues
the task's dependents so that stepping can continue.  This differs from `Run`,
which skips dependents on failure -- the difference gives callers of `RunNext`
full control over whether to abort or continue after an error.

**Concurrency**

`RunNext` returns `ErrTaskInProgress` immediately if called while another
`RunNext` (or `Run`) invocation is still executing.  The flag is released as
soon as the task's `Run` method returns.

### Inspecting the execution plan

```go
func (s *Scheduler) ExecutionPlan() ([]string, error)
```

`ExecutionPlan` returns the tasks in topological order without executing them.
Each entry is a string of the form `"<id>: <name>"`.

```go
plan, err := s.ExecutionPlan()
// plan == ["1: fetch", "2: build", "3: deploy"]
```

Tasks that have no dependency relationship with each other may appear in any
relative order.  `ExecutionPlan` returns `dag.ErrCycle` if the graph contains a
cycle.

### Sentinel errors (scheduler)

| Error | Condition |
|---|---|
| `scheduler.ErrExhausted` | All tasks have been executed; returned by `RunNext`. |
| `scheduler.ErrTaskInProgress` | A concurrent `Run` or `RunNext` call is still executing a task. |

---

## Implementing a custom Task

Use `dag.Func` for simple cases.  For tasks that carry state or need
dependencies injected, implement the `dag.Task` interface directly:

```go
type CompressTask struct {
    id     uint64
    source string
    dest   string
}

func (c *CompressTask) ID() uint64   { return c.id }
func (c *CompressTask) Name() string { return fmt.Sprintf("compress:%s", c.source) }

func (c *CompressTask) Run(ctx context.Context) error {
    f, err := os.Create(c.dest)
    if err != nil {
        return err
    }
    defer f.Close()
    return archiveDir(ctx, c.source, f)
}
```

Registration:

```go
compress := &CompressTask{id: 10, source: "dist/", dest: "dist.tar.gz"}
sign     := dag.Func(11, "sign", signArchive)

_ = s.AddTask(compress)
_ = s.AddTask(sign, compress)
```

---

## Error handling

All sentinel errors are compatible with `errors.Is`:

```go
if err := s.Run(ctx); err != nil {
    switch {
    case errors.Is(err, scheduler.ErrTaskInProgress):
        // a concurrent Run or RunNext is active
    case errors.Is(err, dag.ErrCycle):
        // the graph has a cycle
    default:
        // a task's Run method returned this error
    }
}
```

When using `RunNext` in a loop you can inspect each task error individually and
decide whether to abort or continue:

```go
for {
    err := s.RunNext(ctx)
    switch {
    case errors.Is(err, scheduler.ErrExhausted):
        fmt.Println("all tasks completed")
        return nil
    case errors.Is(err, scheduler.ErrTaskInProgress):
        return fmt.Errorf("concurrent execution detected: %w", err)
    case err != nil:
        log.Printf("task error (continuing): %v", err)
    }
}
```

---

## Concurrency guarantees

- `DAG` is safe for concurrent reads and serialised writes via a `sync.RWMutex`.
- `Scheduler.AddTask` is safe to call from multiple goroutines.
- `Run` and `RunNext` are mutually exclusive: each acquires a shared
  `atomic.Bool` flag using `CompareAndSwap`.  A second concurrent call returns
  `ErrTaskInProgress` immediately rather than blocking.
- Within `Run`, individual task goroutines communicate only through atomic
  in-degree counters; no shared mutable state is accessed after a goroutine is
  launched.

---

## Running the example

The `examples/basic/` directory contains a seven-task CI/CD pipeline that
demonstrates parallel execution across three stages.

```
go run ./examples/basic/
```

---

## Running the tests with the race detector:

```
go test -race ./...
```

Tests are organised as testify suites:

| Suite | Package | Coverage |
|---|---|---|
| `DAGSuite` | `dag` | AddNode, Dependents, InDegrees |
| `TopoSuite` | `dag` | Sort: linear, diamond, disconnected |
| `SchedulerSuite` | `scheduler` | AddTask, Run, RunNext, ExecutionPlan |

---


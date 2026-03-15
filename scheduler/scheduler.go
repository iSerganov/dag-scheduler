// Package scheduler provides a DAG-based parallel task runner.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/iSerganov/dag-scheduler/dag"
)

// ErrExhausted is returned by RunNext when all tasks have already been executed.
var ErrExhausted = errors.New("scheduler: no remaining tasks")

// ErrTaskInProgress is returned by RunNext when a previous task is still running.
var ErrTaskInProgress = errors.New("scheduler: task in progress")

// stepState holds the mutable state used by RunNext between calls.
type stepState struct {
	counters map[uint64]int
	queue    []*dag.Node
}

// Scheduler builds a DAG of tasks and executes them with maximum parallelism:
// a task starts as soon as all its declared dependencies finish.
type Scheduler struct {
	mu      sync.Mutex
	d       *dag.DAG
	byID    map[uint64]*dag.Node
	step    *stepState  // nil until the first RunNext call; reset by AddTask
	running atomic.Bool // true while a RunNext task is executing
}

// New returns a ready-to-use Scheduler.
func New() *Scheduler {
	return &Scheduler{
		d:    dag.New(),
		byID: make(map[uint64]*dag.Node),
	}
}

// AddTask registers t with an optional list of dependency tasks.
// Every dependency must have been added before the task that depends on it.
// Registering a duplicate task ID or referencing an unregistered dependency
// returns an error; the Scheduler remains usable after such an error.
func (s *Scheduler) AddTask(t dag.Task, deps ...dag.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.byID[t.ID()]; ok {
		return fmt.Errorf("scheduler: task %q (id=%d) already registered", t.Name(), t.ID())
	}

	for _, dep := range deps {
		if _, ok := s.byID[dep.ID()]; !ok {
			return fmt.Errorf("scheduler: unknown dependency %q (id=%d) for task %q",
				dep.Name(), dep.ID(), t.Name())
		}
	}

	node := &dag.Node{Task: t, Deps: deps}
	err := s.d.AddNode(node)
	if err != nil {
		return fmt.Errorf("scheduler: %w", err)
	}

	s.byID[t.ID()] = node
	s.step = nil // invalidate any in-progress step state

	return nil
}

// ExecutionPlan returns the tasks in the order they will be executed, as
// strings of the form "<id>: <name>". The ordering is a valid topological sort
// of the DAG; tasks with no dependency relationship may appear in any relative
// order. Returns an error if the graph contains a cycle.
func (s *Scheduler) ExecutionPlan() ([]string, error) {
	sorted, err := s.d.Sort()
	if err != nil {
		return nil, err
	}
	plan := make([]string, len(sorted))
	for i, n := range sorted {
		plan[i] = fmt.Sprintf("%d: %s", n.Task.ID(), n.Task.Name())
	}

	return plan, nil
}

// Run validates the DAG for cycles and then executes all tasks concurrently
// respecting dependency order.
//
// Returns ErrTaskInProgress immediately if a RunNext task is currently executing.
// The first task error is captured and returned after the graph fully drains.
// When a task fails (or ctx is cancelled), its dependents are skipped rather
// than started, so the scheduler always terminates cleanly.
func (s *Scheduler) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return ErrTaskInProgress
	}
	defer s.running.Store(false)

	if _, err := s.d.Sort(); err != nil {
		return err
	}

	rawDeg := s.d.InDegrees()

	// One atomic counter per node tracks remaining unfinished dependencies.
	counters := make(map[uint64]*atomic.Int32, len(rawDeg))
	for id, deg := range rawDeg {
		c := new(atomic.Int32)
		c.Store(int32(deg))
		counters[id] = c
	}

	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		runErr  error
		failed  atomic.Bool
	)

	var runNode func(n *dag.Node)
	runNode = func(n *dag.Node) {
		// Skip execution but still drain the subgraph so the WaitGroup settles.
		if ctx.Err() != nil || failed.Load() {
			s.drainDependents(n, counters, &wg, runNode)

			return
		}

		err := n.Task.Run(ctx)
		if err != nil {
			errOnce.Do(func() { runErr = err })
			failed.Store(true)
		}

		s.drainDependents(n, counters, &wg, runNode)
	}

	for _, n := range s.d.Nodes() {
		if counters[n.Task.ID()].Load() == 0 {
			wg.Go(func() { runNode(n) })
		}
	}

	wg.Wait()

	return runErr
}

// RunNext executes exactly one task that is currently ready (all dependencies
// satisfied) and returns. Repeated calls step through the DAG one task at a
// time in dependency order; any valid topological ordering may be produced when
// multiple tasks are ready simultaneously.
//
// Returns ErrTaskInProgress immediately if called while a previous RunNext task
// is still executing.
// Returns ErrExhausted when every task has already been executed.
// Returns the task's error if the task itself fails; subsequent RunNext calls
// continue from where the queue left off.
func (s *Scheduler) RunNext(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return ErrTaskInProgress
	}
	defer s.running.Store(false)

	s.mu.Lock()

	if s.step == nil {
		if _, err := s.d.Sort(); err != nil {
			s.mu.Unlock()

			return err
		}
		st := &stepState{counters: s.d.InDegrees()}
		for _, n := range s.d.Nodes() {
			if st.counters[n.Task.ID()] == 0 {
				st.queue = append(st.queue, n)
			}
		}
		s.step = st
	}

	if len(s.step.queue) == 0 {
		s.mu.Unlock()

		return ErrExhausted
	}

	n := s.step.queue[0]
	s.step.queue = s.step.queue[1:]
	s.mu.Unlock()

	// Always update the queue so dependents become available on the next call,
	// even when the task itself returns an error.
	taskErr := n.Task.Run(ctx)

	s.mu.Lock()
	// Capture dependents before removing the node (RemoveNode deletes the edge list).
	dependents := s.d.Dependents(n.Task.ID())
	// Remove the completed node so Run sees only the remaining work.
	s.d.RemoveNode(n.Task.ID())
	delete(s.byID, n.Task.ID())
	for _, dep := range dependents {
		id := dep.Task.ID()
		s.step.counters[id]--
		if s.step.counters[id] == 0 {
			s.step.queue = append(s.step.queue, dep)
		}
	}
	s.mu.Unlock()

	return taskErr
}

// drainDependents decrements the in-degree counter of every node that depends
// on n and launches any node whose counter reaches zero.
func (s *Scheduler) drainDependents(
	n *dag.Node,
	counters map[uint64]*atomic.Int32,
	wg *sync.WaitGroup,
	runNode func(*dag.Node),
) {
	for _, dep := range s.d.Dependents(n.Task.ID()) {
		if counters[dep.Task.ID()].Add(-1) == 0 {
			wg.Go(func() { runNode(dep) })
		}
	}
}

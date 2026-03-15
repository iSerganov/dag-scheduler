package scheduler_test

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/brianvoe/gofakeit/v7"

	"github.com/iSerganov/dag-scheduler/dag"
	"github.com/iSerganov/dag-scheduler/scheduler"
)

// taskDef bundles a Task together with its pre-resolved dependency slice so the
// benchmark loop can rebuild a fresh Scheduler each iteration without
// regenerating the graph structure.
type taskDef struct {
	task dag.Task
	deps []dag.Task
}

// BenchmarkRun_1000Tasks measures the combined cost of registering 1 000 tasks
// (most with 2–10 dependencies) and executing the full DAG.
//
// Graph layout:
//   - Tasks  1–10  are roots (no deps).
//   - Tasks 11–1000 each depend on 2–10 tasks chosen uniformly at random from
//     all tasks added before them, guaranteeing an acyclic graph.
//
// Each task writes
//
//	"task [ID: %d, Name: %s] has successfully finished"
//
// to io.Discard to simulate realistic work without polluting benchmark output.
func BenchmarkRun_1000Tasks(b *testing.B) {
	const (
		numTasks = 1000
		numRoots = 10
		minDeps  = 2
		maxDeps  = 10
	)

	// ── Build task objects and dependency lists once ──────────────────────────
	defs := make([]taskDef, numTasks)
	for i := range numTasks {
		id := uint64(i + 1)
		name := gofakeit.HipsterWord()

		defs[i].task = dag.Func(id, name, func(_ context.Context) error {
			_, err := fmt.Fprintf(io.Discard, "task [ID: %d, Name: %s] has successfully finished\n", id, name)

			return err
		})

		if i < numRoots {
			continue // roots have no deps
		}

		// Pick between minDeps and maxDeps unique predecessors.
		n := minDeps + rand.IntN(maxDeps-minDeps+1)
		defs[i].deps = sampleDeps(defs[:i], n)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		// Exclude Scheduler construction from the timed region.
		b.StopTimer()
		s := scheduler.New()
		b.StartTimer()

		for _, def := range defs {
			if err := s.AddTask(def.task, def.deps...); err != nil {
				b.Fatalf("AddTask: %v", err)
			}
		}
		if err := s.Run(context.Background()); err != nil {
			b.Fatalf("Run: %v", err)
		}
	}
}

// sampleDeps returns n distinct dag.Task values drawn at random from pool
// using a partial Fisher-Yates shuffle. If n ≥ len(pool) the whole pool is
// returned.
func sampleDeps(pool []taskDef, n int) []dag.Task {
	if n >= len(pool) {
		out := make([]dag.Task, len(pool))
		for i, d := range pool {
			out[i] = d.task
		}

		return out
	}

	// Work on a temporary index slice to avoid mutating pool.
	indices := make([]int, len(pool))
	for i := range indices {
		indices[i] = i
	}
	for i := range n {
		j := i + rand.IntN(len(pool)-i)
		indices[i], indices[j] = indices[j], indices[i]
	}

	out := make([]dag.Task, n)
	for i := range n {
		out[i] = pool[indices[i]].task
	}

	return out
}

package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/iSerganov/dag-scheduler/dag"
	"github.com/iSerganov/dag-scheduler/scheduler"
)

type SchedulerSuite struct {
	suite.Suite
}

func TestSchedulerSuite(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}

// mustAdd is a helper that calls AddTask and fails the test on error.
func (s *SchedulerSuite) mustAdd(sch *scheduler.Scheduler, t dag.Task, deps ...dag.Task) {
	s.T().Helper()
	s.Require().NoError(sch.AddTask(t, deps...))
}

// assertBefore asserts that dep appears before task in plan (keyed by "<id>: <name>").
func (s *SchedulerSuite) assertBefore(pos map[string]int, dep, task dag.Task) {
	s.T().Helper()
	depEntry := fmt.Sprintf("%d: %s", dep.ID(), dep.Name())
	taskEntry := fmt.Sprintf("%d: %s", task.ID(), task.Name())
	di, dok := pos[depEntry]
	ti, tok := pos[taskEntry]
	s.True(dok, "entry %q missing from plan", depEntry)
	s.True(tok, "entry %q missing from plan", taskEntry)
	if dok && tok {
		s.Less(di, ti, "want %q before %q", depEntry, taskEntry)
	}
}

// ── Run ──────────────────────────────────────────────────────────────────────

func (s *SchedulerSuite) TestRun_LinearChain() {
	sch := scheduler.New()
	var order []string
	var mu sync.Mutex

	a := dag.Func(1, "a", func(_ context.Context) error { mu.Lock(); order = append(order, "a"); mu.Unlock(); return nil })
	b := dag.Func(2, "b", func(_ context.Context) error { mu.Lock(); order = append(order, "b"); mu.Unlock(); return nil })
	c := dag.Func(3, "c", func(_ context.Context) error { mu.Lock(); order = append(order, "c"); mu.Unlock(); return nil })

	s.mustAdd(sch, a)
	s.mustAdd(sch, b, a)
	s.mustAdd(sch, c, b)

	s.Require().NoError(sch.Run(context.Background()))
	s.Equal([]string{"a", "b", "c"}, order)
}

func (s *SchedulerSuite) TestRun_ParallelBranches() {
	// a → b
	// a → c
	// b, c → d
	sch := scheduler.New()
	const delay = 20 * time.Millisecond
	var bStart, cStart time.Time
	var mu sync.Mutex

	a := dag.Func(1, "a", func(_ context.Context) error { return nil })
	b := dag.Func(2, "b", func(_ context.Context) error {
		mu.Lock()
		bStart = time.Now()
		mu.Unlock()
		time.Sleep(delay)
		return nil
	})
	c := dag.Func(3, "c", func(_ context.Context) error {
		mu.Lock()
		cStart = time.Now()
		mu.Unlock()
		time.Sleep(delay)
		return nil
	})
	d := dag.Func(4, "d", func(_ context.Context) error { return nil })

	s.mustAdd(sch, a)
	s.mustAdd(sch, b, a)
	s.mustAdd(sch, c, a)
	s.mustAdd(sch, d, b, c)

	start := time.Now()
	s.Require().NoError(sch.Run(context.Background()))
	elapsed := time.Since(start)

	mu.Lock()
	skew := bStart.Sub(cStart)
	if skew < 0 {
		skew = -skew
	}
	mu.Unlock()
	s.LessOrEqual(skew, 5*time.Millisecond, "b and c should start in parallel")
	s.LessOrEqual(elapsed, 3*delay, "total time should be ≈ one delay, not two")
}

func (s *SchedulerSuite) TestRun_TaskError_PropagatesAndSkipsDependents() {
	sch := scheduler.New()
	boom := errors.New("boom")
	var ran atomic.Bool

	a := dag.Func(1, "a", func(_ context.Context) error { return boom })
	b := dag.Func(2, "b", func(_ context.Context) error { ran.Store(true); return nil })

	s.mustAdd(sch, a)
	s.mustAdd(sch, b, a)

	s.ErrorIs(sch.Run(context.Background()), boom)
	s.False(ran.Load(), "dependent task should have been skipped after error")
}

func (s *SchedulerSuite) TestRun_ContextCancellation() {
	sch := scheduler.New()
	ctx, cancel := context.WithCancel(context.Background())

	a := dag.Func(1, "a", func(_ context.Context) error { cancel(); return nil })
	b := dag.Func(2, "b", func(_ context.Context) error {
		s.Fail("b should not run after context cancellation")
		return nil
	})

	s.mustAdd(sch, a)
	s.mustAdd(sch, b, a)
	_ = sch.Run(ctx)
}

func (s *SchedulerSuite) TestRun_Empty() {
	s.NoError(scheduler.New().Run(context.Background()))
}

func (s *SchedulerSuite) TestRun_SingleTask() {
	sch := scheduler.New()
	ran := false
	s.mustAdd(sch, dag.Func(1, "only", func(_ context.Context) error { ran = true; return nil }))
	s.Require().NoError(sch.Run(context.Background()))
	s.True(ran)
}

func (s *SchedulerSuite) TestRun_TaskInProgress() {
	sch := scheduler.New()
	started := make(chan struct{})
	unblock := make(chan struct{})

	s.mustAdd(sch, dag.Func(1, "a", func(_ context.Context) error {
		close(started)
		<-unblock
		return nil
	}))

	go sch.Run(context.Background()) //nolint:errcheck
	<-started

	s.ErrorIs(sch.Run(context.Background()), scheduler.ErrTaskInProgress, "Run during Run")
	s.ErrorIs(sch.RunNext(context.Background()), scheduler.ErrTaskInProgress, "RunNext during Run")
	close(unblock)
}

// ── AddTask ───────────────────────────────────────────────────────────────────

func (s *SchedulerSuite) TestAddTask_DuplicateID() {
	sch := scheduler.New()
	a := dag.Func(1, "a", func(_ context.Context) error { return nil })
	s.mustAdd(sch, a)
	s.Error(sch.AddTask(a), "expected error for duplicate task ID")
}

func (s *SchedulerSuite) TestAddTask_UnknownDependency() {
	sch := scheduler.New()
	ghost := dag.Func(99, "ghost", func(_ context.Context) error { return nil })
	a := dag.Func(1, "a", func(_ context.Context) error { return nil })
	s.Error(sch.AddTask(a, ghost), "expected error for unknown dependency")
}

// ── RunNext ───────────────────────────────────────────────────────────────────

func (s *SchedulerSuite) TestRunNext_LinearChain() {
	sch := scheduler.New()
	var order []string

	a := dag.Func(1, "a", func(_ context.Context) error { order = append(order, "a"); return nil })
	b := dag.Func(2, "b", func(_ context.Context) error { order = append(order, "b"); return nil })
	c := dag.Func(3, "c", func(_ context.Context) error { order = append(order, "c"); return nil })

	s.mustAdd(sch, a)
	s.mustAdd(sch, b, a)
	s.mustAdd(sch, c, b)

	for i := range 3 {
		s.Require().NoError(sch.RunNext(context.Background()), "step %d", i)
	}
	s.ErrorIs(sch.RunNext(context.Background()), scheduler.ErrExhausted)
	s.Equal([]string{"a", "b", "c"}, order)
}

func (s *SchedulerSuite) TestRunNext_ExhaustedOnEmpty() {
	s.ErrorIs(scheduler.New().RunNext(context.Background()), scheduler.ErrExhausted)
}

func (s *SchedulerSuite) TestRunNext_TaskError_StillAdvancesQueue() {
	sch := scheduler.New()
	boom := errors.New("boom")

	a := dag.Func(1, "a", func(_ context.Context) error { return boom })
	b := dag.Func(2, "b", func(_ context.Context) error { return nil })

	s.mustAdd(sch, a)
	s.mustAdd(sch, b, a)

	// a fails but its dependents are still enqueued.
	s.ErrorIs(sch.RunNext(context.Background()), boom)
	// b is now ready.
	s.NoError(sch.RunNext(context.Background()))
	s.ErrorIs(sch.RunNext(context.Background()), scheduler.ErrExhausted)
}

func (s *SchedulerSuite) TestRunNext_TaskInProgress() {
	sch := scheduler.New()
	started := make(chan struct{})
	unblock := make(chan struct{})

	s.mustAdd(sch, dag.Func(1, "a", func(_ context.Context) error {
		close(started)
		<-unblock
		return nil
	}))

	go sch.RunNext(context.Background()) //nolint:errcheck
	<-started

	s.ErrorIs(sch.RunNext(context.Background()), scheduler.ErrTaskInProgress)
	close(unblock)
}

func (s *SchedulerSuite) TestRunNext_ThenRun_DoesNotRepeatTasks() {
	sch := scheduler.New()
	var mu sync.Mutex
	executed := []string{}
	record := func(name string) func(context.Context) error {
		return func(_ context.Context) error {
			mu.Lock()
			executed = append(executed, name)
			mu.Unlock()
			return nil
		}
	}

	// a -> b -> c -> d
	a := dag.Func(1, "a", record("a"))
	b := dag.Func(2, "b", record("b"))
	c := dag.Func(3, "c", record("c"))
	d := dag.Func(4, "d", record("d"))
	s.mustAdd(sch, a)
	s.mustAdd(sch, b, a)
	s.mustAdd(sch, c, b)
	s.mustAdd(sch, d, c)

	// Step through the first two tasks manually.
	s.Require().NoError(sch.RunNext(context.Background())) // runs a
	s.Require().NoError(sch.RunNext(context.Background())) // runs b

	// Run should complete only the remaining tasks: c and d.
	s.Require().NoError(sch.Run(context.Background()))

	s.Equal([]string{"a", "b", "c", "d"}, executed,
		"each task must be executed exactly once")
}

func (s *SchedulerSuite) TestRunNext_ResetAfterAddTask() {
	sch := scheduler.New()
	a := dag.Func(1, "a", func(_ context.Context) error { return nil })
	b := dag.Func(2, "b", func(_ context.Context) error { return nil })
	s.mustAdd(sch, a)
	s.mustAdd(sch, b, a)

	// Run a; b becomes ready.
	s.Require().NoError(sch.RunNext(context.Background()))

	// Add an independent task — this resets the step state so the new task
	// is included in the next RunNext calls.
	c := dag.Func(3, "c", func(_ context.Context) error { return nil })
	s.mustAdd(sch, c)

	// Two tasks remain (b and c); both should run without error.
	s.NoError(sch.RunNext(context.Background()))
	s.NoError(sch.RunNext(context.Background()))
	s.ErrorIs(sch.RunNext(context.Background()), scheduler.ErrExhausted)
}

// ── ExecutionPlan ─────────────────────────────────────────────────────────────

// TestExecutionPlan_Complex builds a 17-task build-pipeline DAG and verifies
// that ExecutionPlan returns every task in a valid dependency order.
//
// Dependency graph (→ means "depends on"):
//
//	Stage 0 — roots
//	  1:init-config
//	  2:fetch-deps
//	  3:fetch-assets
//	  4:fetch-schema
//
//	Stage 1
//	  5:validate-config  → 1
//	  6:compile-core     → 1, 2
//	  7:gen-types        → 2, 4
//	  8:process-assets   → 1, 3
//
//	Stage 2
//	  9:lint             → 5, 6
//	  10:compile-plugins → 2, 6, 7         (3 deps)
//	  11:optimize-assets → 7, 8
//
//	Stage 3
//	  12:test-unit       → 9, 10
//	  13:test-integ      → 7, 10, 11       (3 deps)
//	  14:bundle          → 6, 10, 11       (3 deps)
//
//	Stage 4
//	  15:sign            → 9, 12, 13, 14   (4 deps)
//	  16:publish         → 15
//	  17:notify          → 13, 16
func (s *SchedulerSuite) TestExecutionPlan_Complex() {
	noop := func(_ context.Context) error { return nil }

	// Stage 0 — roots
	initConfig := dag.Func(1, "init-config", noop)
	fetchDeps := dag.Func(2, "fetch-deps", noop)
	fetchAssets := dag.Func(3, "fetch-assets", noop)
	fetchSchema := dag.Func(4, "fetch-schema", noop)
	// Stage 1
	validateCfg := dag.Func(5, "validate-config", noop)
	compileCore := dag.Func(6, "compile-core", noop)
	genTypes := dag.Func(7, "gen-types", noop)
	procAssets := dag.Func(8, "process-assets", noop)
	// Stage 2
	lint := dag.Func(9, "lint", noop)
	compPlugins := dag.Func(10, "compile-plugins", noop)
	optAssets := dag.Func(11, "optimize-assets", noop)
	// Stage 3
	testUnit := dag.Func(12, "test-unit", noop)
	testInteg := dag.Func(13, "test-integ", noop)
	bundle := dag.Func(14, "bundle", noop)
	// Stage 4
	sign := dag.Func(15, "sign", noop)
	publish := dag.Func(16, "publish", noop)
	notify := dag.Func(17, "notify", noop)

	sch := scheduler.New()
	s.mustAdd(sch, initConfig)
	s.mustAdd(sch, fetchDeps)
	s.mustAdd(sch, fetchAssets)
	s.mustAdd(sch, fetchSchema)
	s.mustAdd(sch, validateCfg, initConfig)
	s.mustAdd(sch, compileCore, initConfig, fetchDeps)
	s.mustAdd(sch, genTypes, fetchDeps, fetchSchema)
	s.mustAdd(sch, procAssets, initConfig, fetchAssets)
	s.mustAdd(sch, lint, validateCfg, compileCore)
	s.mustAdd(sch, compPlugins, fetchDeps, compileCore, genTypes) // 3 deps
	s.mustAdd(sch, optAssets, genTypes, procAssets)
	s.mustAdd(sch, testUnit, lint, compPlugins)
	s.mustAdd(sch, testInteg, genTypes, compPlugins, optAssets) // 3 deps
	s.mustAdd(sch, bundle, compileCore, compPlugins, optAssets) // 3 deps
	s.mustAdd(sch, sign, lint, testUnit, testInteg, bundle)     // 4 deps
	s.mustAdd(sch, publish, sign)
	s.mustAdd(sch, notify, testInteg, publish)

	plan, err := sch.ExecutionPlan()
	s.Require().NoError(err)
	s.Require().Len(plan, 17)

	pos := make(map[string]int, len(plan))
	for i, entry := range plan {
		pos[entry] = i
	}

	s.assertBefore(pos, initConfig, validateCfg)
	s.assertBefore(pos, initConfig, compileCore)
	s.assertBefore(pos, fetchDeps, compileCore)
	s.assertBefore(pos, fetchDeps, genTypes)
	s.assertBefore(pos, fetchSchema, genTypes)
	s.assertBefore(pos, initConfig, procAssets)
	s.assertBefore(pos, fetchAssets, procAssets)
	s.assertBefore(pos, validateCfg, lint)
	s.assertBefore(pos, compileCore, lint)
	s.assertBefore(pos, fetchDeps, compPlugins)
	s.assertBefore(pos, compileCore, compPlugins)
	s.assertBefore(pos, genTypes, compPlugins)
	s.assertBefore(pos, genTypes, optAssets)
	s.assertBefore(pos, procAssets, optAssets)
	s.assertBefore(pos, lint, testUnit)
	s.assertBefore(pos, compPlugins, testUnit)
	s.assertBefore(pos, genTypes, testInteg)
	s.assertBefore(pos, compPlugins, testInteg)
	s.assertBefore(pos, optAssets, testInteg)
	s.assertBefore(pos, compileCore, bundle)
	s.assertBefore(pos, compPlugins, bundle)
	s.assertBefore(pos, optAssets, bundle)
	s.assertBefore(pos, lint, sign)
	s.assertBefore(pos, testUnit, sign)
	s.assertBefore(pos, testInteg, sign)
	s.assertBefore(pos, bundle, sign)
	s.assertBefore(pos, sign, publish)
	s.assertBefore(pos, testInteg, notify)
	s.assertBefore(pos, publish, notify)
}

func (s *SchedulerSuite) TestExecutionPlan_Empty() {
	plan, err := scheduler.New().ExecutionPlan()
	s.Require().NoError(err)
	s.Empty(plan)
}

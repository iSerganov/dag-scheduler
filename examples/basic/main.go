// Package main demonstrates a three-stage CI/CD pipeline built with dag-scheduler.
//
// Pipeline layout:
//
//	Stage 1   				checkout
//	                            |
//	 				 +----------+----------+
//			     	 |          |          |
//	Stage 2        test        lint        build
//	                             |         |
//	                             |       / |
//	                             |      /  |
//	                             |     /   |
//	                             |    /    |
//	                             |   /     |
//	                             |  /      |
//	                             | /       |
//	                             |/        |
//	Stage 3             security-scan  docker-build
//	                          \             /
//	                           \           /
//	Stage 4             test ----> deploy <---- (security-scan, docker-build)
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/iSerganov/dag-scheduler/dag"
	"github.com/iSerganov/dag-scheduler/scheduler"
)

func main() {
	// Stage 1 -- source
	checkout := task(1, "checkout", "stage-1", func() {
		simulate(300*time.Millisecond, "cloning repository")
	})

	// Stage 2 -- validation and compilation (all depend on checkout, run in parallel)
	test := task(2, "test", "stage-2", func() {
		simulate(600*time.Millisecond, "running unit tests")
	})
	lint := task(3, "lint", "stage-2", func() {
		simulate(400*time.Millisecond, "linting source code")
	})
	build := task(4, "build", "stage-2", func() {
		simulate(700*time.Millisecond, "compiling binaries")
	})

	// Stage 3 -- packaging and release
	securityScan := task(5, "security-scan", "stage-3", func() {
		simulate(500*time.Millisecond, "scanning for vulnerabilities")
	})
	dockerBuild := task(6, "docker-build", "stage-3", func() {
		simulate(800*time.Millisecond, "building container image")
	})
	deploy := task(7, "deploy", "stage-4", func() {
		simulate(400*time.Millisecond, "deploying to production")
	})

	s := scheduler.New()
	mustAdd(s, checkout)
	mustAdd(s, test, checkout)
	mustAdd(s, lint, checkout)
	mustAdd(s, build, checkout)
	mustAdd(s, securityScan, lint, build) // 2 deps
	mustAdd(s, dockerBuild, build)
	mustAdd(s, deploy, test, securityScan, dockerBuild) // 3 deps

	printPlan(s)

	fmt.Println(strings.Repeat("-", 52))
	fmt.Println("Running pipeline")
	fmt.Println(strings.Repeat("-", 52))

	start := time.Now()
	// single task execution
	if err := s.RunNext(context.Background()); err != nil {
		log.Fatal(err)
	}
	// full concurrent execution
	if err := s.Run(context.Background()); err != nil {
		log.Fatal(err)
	}

	fmt.Println(strings.Repeat("-", 52))
	fmt.Printf("Pipeline finished in %s\n", time.Since(start).Round(time.Millisecond))
}

// task creates a named dag.Task that logs start/finish and calls work.
func task(id uint64, name, stage string, work func()) dag.Task {
	return dag.Func(id, name, func(_ context.Context) error {
		fmt.Printf("[%-7s] %-14s  started\n", stage, name)
		work()
		fmt.Printf("[%-7s] %-14s  done\n", stage, name)
		return nil
	})
}

// simulate sleeps for d to represent real work and prints a description.
func simulate(d time.Duration, description string) {
	fmt.Printf("             %-14s  %s...\n", "", description)
	time.Sleep(d)
}

// printPlan prints the scheduled execution order without running anything.
func printPlan(s *scheduler.Scheduler) {
	plan, err := s.ExecutionPlan()
	if err != nil {
		log.Fatalf("execution plan: %v", err)
	}
	fmt.Println(strings.Repeat("-", 52))
	fmt.Println("Execution plan (topological order)")
	fmt.Println(strings.Repeat("-", 52))
	for _, entry := range plan {
		fmt.Printf("%s\n", entry)
	}
}

// mustAdd registers a task with the scheduler and exits on error.
func mustAdd(s *scheduler.Scheduler, t dag.Task, deps ...dag.Task) {
	if err := s.AddTask(t, deps...); err != nil {
		log.Fatalf("add %s: %v", t.Name(), err)
	}
}

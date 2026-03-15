package dag

import (
	"fmt"
)

// Sort returns all nodes in a valid topological order using Kahn's algorithm
// (O(V + E)), or returns ErrCycle if the graph is not acyclic.
//
// A topological ordering guarantees that for every edge A → B (B depends on A),
// A appears before B in the result.  The algorithm works in three phases:
//
// Phase 1 — seed the queue.
// Every node whose in-degree is 0 (no unmet dependencies) is immediately safe
// to visit and is placed into the queue.  The in-degree snapshot is a copy of
// the DAG's internal map, so the graph itself is never mutated.
//
// Phase 2 — BFS drain loop.
// Nodes are dequeued one at a time and appended to the result slice.  For each
// dequeued node, every dependent (node that listed it as a dependency) has its
// in-degree decremented by 1.  The moment a dependent's in-degree reaches 0,
// all of its prerequisites have been satisfied and it is added to the queue.
// Each node enters the queue exactly once — the instant its last dependency is
// resolved — which preserves the ordering invariant.
//
// Phase 3 — cycle detection.
// Nodes that form a cycle can never reach in-degree 0 because each is always
// waiting for another member of the same cycle to finish first.  If the result
// slice is shorter than the total node count, the missing nodes are part of a
// cycle and ErrCycle is returned.
func (d *DAG) Sort() ([]*Node, error) {
	nodes := d.Nodes()
	n := len(nodes)
	inDeg := d.InDegrees()

	queue := make([]*Node, 0, n)
	for _, node := range nodes {
		if inDeg[node.Task.ID()] == 0 {
			queue = append(queue, node)
		}
	}

	sorted := make([]*Node, 0, n)
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		sorted = append(sorted, curr)

		for _, dependent := range d.Dependents(curr.Task.ID()) {
			id := dependent.Task.ID()
			inDeg[id]--
			if inDeg[id] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	if len(sorted) != n {
		return nil, fmt.Errorf("%w: %d node(s) involved", ErrCycle, n-len(sorted))
	}

	return sorted, nil
}

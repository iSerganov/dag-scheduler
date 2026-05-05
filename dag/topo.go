package dag

import (
	"fmt"
	"maps"
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
//
// The entire graph state is snapshotted under a single read lock so concurrent
// AddNode/RemoveNode calls cannot produce an inconsistent view.
func (d *DAG) Sort() ([]*Node, error) {
	sorted, _, _, err := d.SortAndSnapshot()

	return sorted, err
}

// SortAndSnapshot returns the same topological order as Sort along with two
// additional snapshots captured under the same single read lock:
//
//   - inDeg: a writable copy of each node's dependency count at the moment of
//     the call (the original in-degrees, not mutated by Kahn's algorithm).
//   - dependents: a map from each node ID to the slice of nodes that directly
//     depend on it — equivalent to calling Dependents for every node but with
//     only one lock acquisition total.
//
// This method is intended for callers that need all three pieces of data (e.g.
// Scheduler.Run), where calling Sort, InDegrees, and Dependents separately
// would re-acquire the read lock on each call.
func (d *DAG) SortAndSnapshot() ([]*Node, map[uint64]int, map[uint64][]*Node, error) {
	// Snapshot everything under a single read lock.
	d.mu.RLock()
	n := len(d.nodes)
	nodeMap := make(map[uint64]*Node, n)

	for id, node := range d.nodes {
		nodeMap[id] = node
	}

	inDeg := make(map[uint64]int, len(d.inDeg))
	maps.Copy(inDeg, d.inDeg)

	adjIDs := make(map[uint64][]uint64, len(d.adj))

	for id, succs := range d.adj {
		cp := make([]uint64, len(succs))
		copy(cp, succs)
		adjIDs[id] = cp
	}

	d.mu.RUnlock()

	// Build the dependents map (id → []*Node) from the adj-ID snapshot so the
	// caller never has to call Dependents in a separate lock acquisition.
	dependents := make(map[uint64][]*Node, len(adjIDs))

	for id, succIDs := range adjIDs {
		if len(succIDs) > 0 {
			nodes := make([]*Node, len(succIDs))
			for i, sid := range succIDs {
				nodes[i] = nodeMap[sid]
			}

			dependents[id] = nodes
		}
	}

	// Run Kahn on a work copy so inDeg remains an unmodified snapshot for
	// the caller.
	inDegWork := maps.Clone(inDeg)

	// Phase 1 — seed with zero in-degree nodes.
	queue := make([]*Node, 0, n)

	for id, node := range nodeMap {
		if inDegWork[id] == 0 {
			queue = append(queue, node)
		}
	}

	// Phase 2 — BFS drain using an index pointer (O(1) dequeue, no slice shift).
	sorted := make([]*Node, 0, n)
	head := 0

	for head < len(queue) {
		curr := queue[head]
		head++
		sorted = append(sorted, curr)

		for _, succID := range adjIDs[curr.Task.ID()] {
			inDegWork[succID]--
			if inDegWork[succID] == 0 {
				queue = append(queue, nodeMap[succID])
			}
		}
	}

	// Phase 3 — cycle detection.
	if len(sorted) != n {
		return nil, nil, nil, fmt.Errorf("%w: %d node(s) involved", ErrCycle, n-len(sorted))
	}

	return sorted, inDeg, dependents, nil
}

// Package dag provides a thread-safe directed acyclic graph of Nodes.
package dag

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
)

var (
	// ErrNodeExists is returned when a node with the same ID is already in the DAG.
	ErrNodeExists = errors.New("node already exists")
	// ErrNodeNotFound is returned when a referenced dependency is not in the DAG.
	ErrNodeNotFound = errors.New("node not found")
	// ErrCycle is returned when a cycle is detected in the graph.
	ErrCycle = errors.New("cycle detected")
)

// DAG is a thread-safe directed acyclic graph of Nodes.
//
// Edges flow from dependency to dependent: if B declares A as a dependency,
// the internal edge is A → B, so A is visited before B during traversal.
type DAG struct {
	mu    sync.RWMutex
	nodes map[uint64]*Node
	// adj maps a node ID to the IDs of nodes that depend on it (outgoing edges).
	adj   map[uint64][]uint64
	inDeg map[uint64]int
}

// New returns an empty, ready-to-use DAG.
func New() *DAG {
	return &DAG{
		nodes: make(map[uint64]*Node),
		adj:   make(map[uint64][]uint64),
		inDeg: make(map[uint64]int),
	}
}

// AddNode registers n and wires edges from each of its declared dependencies.
//
// Every dependency listed in n.Deps must already be present in the DAG.
// Returns ErrNodeExists if a node with the same Task ID is already registered.
// Returns ErrNodeNotFound if any dependency is unknown.
// Returns an error if n, n.Task, or any entry in n.Deps is nil.
// Duplicate entries in n.Deps are silently deduplicated.
func (d *DAG) AddNode(n *Node) error {
	if n == nil {
		return errors.New("node must not be nil")
	}

	if n.Task == nil {
		return errors.New("node task must not be nil")
	}

	for i, dep := range n.Deps {
		if dep == nil {
			return fmt.Errorf("dependency at index %d must not be nil", i)
		}
	}

	id := n.Task.ID()

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.nodes[id]; ok {
		return fmt.Errorf("%w: id=%d name=%s", ErrNodeExists, id, n.Task.Name())
	}

	// Deduplicate deps, preserving first-seen order.
	seen := make(map[uint64]struct{}, len(n.Deps))
	dedupedDeps := make([]Task, 0, len(n.Deps))

	for _, dep := range n.Deps {
		if _, dup := seen[dep.ID()]; dup {
			continue
		}

		seen[dep.ID()] = struct{}{}

		if _, ok := d.nodes[dep.ID()]; !ok {
			return fmt.Errorf("%w: dependency id=%d name=%s not found for node id=%d name=%s",
				ErrNodeNotFound, dep.ID(), dep.Name(), id, n.Task.Name())
		}

		dedupedDeps = append(dedupedDeps, dep)
	}

	n.Deps = dedupedDeps

	d.nodes[id] = n
	d.inDeg[id] = len(n.Deps)

	if _, ok := d.adj[id]; !ok {
		d.adj[id] = nil
	}

	for _, dep := range n.Deps {
		depID := dep.ID()
		d.adj[depID] = append(d.adj[depID], id)
	}

	return nil
}

// Node returns the node with the given ID and whether it was found.
func (d *DAG) Node(id uint64) (*Node, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	n, ok := d.nodes[id]

	return n, ok
}

// Nodes returns all nodes (order unspecified).
func (d *DAG) Nodes() []*Node {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]*Node, 0, len(d.nodes))
	for _, n := range d.nodes {
		out = append(out, n)
	}

	return out
}

// Dependents returns the nodes that directly depend on the node with id.
func (d *DAG) Dependents(id uint64) []*Node {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ids := d.adj[id]
	out := make([]*Node, 0, len(ids))
	for _, depID := range ids {
		out = append(out, d.nodes[depID])
	}

	return out
}

// InDegrees returns a snapshot of the dependency count for every node.
// The returned map is safe to mutate; it does not alias internal state.
func (d *DAG) InDegrees() map[uint64]int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[uint64]int, len(d.inDeg))
	maps.Copy(out, d.inDeg)

	return out
}

// RemoveNode removes the node with the given id from the DAG and updates all
// related state so the graph remains consistent:
//
//   - The in-degree of every dependent (node that listed id as a dependency)
//     is decremented.
//   - id is removed from the adjacency list of every predecessor (node that id
//     depended on), so Sort never encounters a stale reference.
//   - id is removed from the Deps slice of every surviving dependent.
//
// If id is not present, RemoveNode is a no-op.
func (d *DAG) RemoveNode(id uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	n, ok := d.nodes[id]
	if !ok {
		return
	}

	// Save the direct-dependent IDs before step 3 deletes d.adj[id].
	// Only these nodes can have id in their Deps slice, so step 4 iterates
	// them instead of all surviving nodes (O(out-degree) vs O(V)).
	directDeps := d.adj[id]

	// 1. Decrement in-degrees of every node that depended on id.
	for _, depID := range directDeps {
		d.inDeg[depID]--
	}

	// 2. Remove id from the adjacency list of each predecessor so that Sort
	//    does not follow a dangling edge to a deleted node.
	for _, pred := range n.Deps {
		predID := pred.ID()
		d.adj[predID] = slices.DeleteFunc(d.adj[predID], func(x uint64) bool {
			return x == id
		})
	}

	// 3. Remove the node itself.
	delete(d.nodes, id)
	delete(d.adj, id)
	delete(d.inDeg, id)

	// 4. Remove id from the Deps slice of its direct dependents only.
	for _, depID := range directDeps {
		if depNode, ok := d.nodes[depID]; ok {
			depNode.Deps = slices.DeleteFunc(depNode.Deps, func(dep Task) bool {
				return dep.ID() == id
			})
		}
	}
}

// Len returns the number of nodes in the DAG.
func (d *DAG) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return len(d.nodes)
}

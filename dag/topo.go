package dag

import (
	"fmt"
)

// Sort returns the nodes of d in a valid topological order using Kahn's
// algorithm (O(V + E)).  It returns dag.ErrCycle if the graph is not acyclic.
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

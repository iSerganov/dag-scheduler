package dag

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

// stask creates a no-op Task with the given id and name. Defined here and
// shared with topo_test.go because both live in package dag.
func stask(id uint64, name string) *FuncTask {
	return Func(id, name, func(_ context.Context) error { return nil })
}

type DAGSuite struct {
	suite.Suite
}

func TestDAGSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DAGSuite))
}

func (s *DAGSuite) TestAddNode_Basic() {
	d := New()
	s.Require().NoError(d.AddNode(&Node{Task: stask(1, "a")}))
	s.Equal(1, d.Len())
}

func (s *DAGSuite) TestAddNode_DuplicateID() {
	d := New()
	_ = d.AddNode(&Node{Task: stask(1, "a")})
	err := d.AddNode(&Node{Task: stask(1, "a2")})
	s.Require().Error(err)
	s.ErrorIs(err, ErrNodeExists)
}

func (s *DAGSuite) TestAddNode_UnknownDep() {
	d := New()
	missing := stask(99, "missing")
	err := d.AddNode(&Node{Task: stask(1, "a"), Deps: []Task{missing}})
	s.Require().Error(err)
	s.ErrorIs(err, ErrNodeNotFound)
}

func (s *DAGSuite) TestDependents() {
	d := New()
	a := stask(1, "a")
	b := stask(2, "b")
	_ = d.AddNode(&Node{Task: a})
	_ = d.AddNode(&Node{Task: b, Deps: []Task{a}})

	deps := d.Dependents(a.ID())
	s.Require().Len(deps, 1)
	s.Equal(b.ID(), deps[0].Task.ID())
}

//nolint:funlen
func (s *DAGSuite) TestRemoveNode() {
	// nodeSpec describes one node: the task ID and the IDs it depends on.
	type nodeSpec struct {
		id     uint64
		depIDs []uint64
	}
	// wantNode is the expected state of one surviving node after all removals.
	type wantNode struct {
		id         uint64
		inDeg      int
		remainDeps []uint64 // IDs that must still be in node.Deps
	}
	type tc struct {
		name      string
		nodes     []nodeSpec // graph is built in this order
		removeIDs []uint64   // IDs removed sequentially
		wantLen   int
		wantNodes []wantNode
	}

	cases := []tc{
		{
			// 1 → 2 → 3   (read: 2 depends on 1, 3 depends on 2)
			// Remove the root.  Node 2's in-degree drops to 0; node 3 is unchanged.
			name:      "linear/remove root",
			nodes:     []nodeSpec{{1, nil}, {2, []uint64{1}}, {3, []uint64{2}}},
			removeIDs: []uint64{1},
			wantLen:   2,
			wantNodes: []wantNode{
				{id: 2, inDeg: 0, remainDeps: nil},
				{id: 3, inDeg: 1, remainDeps: []uint64{2}},
			},
		},
		{
			// 1 → 2 → 3
			// Remove the middle node.  Node 3's in-degree drops to 0 and its
			// Deps slice must no longer reference node 2.
			name:      "linear/remove middle",
			nodes:     []nodeSpec{{1, nil}, {2, []uint64{1}}, {3, []uint64{2}}},
			removeIDs: []uint64{2},
			wantLen:   2,
			wantNodes: []wantNode{
				{id: 1, inDeg: 0, remainDeps: nil},
				{id: 3, inDeg: 0, remainDeps: nil},
			},
		},
		{
			// 1 → 2 → 3
			// Remove the leaf.  Nothing else changes.
			name:      "linear/remove leaf",
			nodes:     []nodeSpec{{1, nil}, {2, []uint64{1}}, {3, []uint64{2}}},
			removeIDs: []uint64{3},
			wantLen:   2,
			wantNodes: []wantNode{
				{id: 1, inDeg: 0, remainDeps: nil},
				{id: 2, inDeg: 1, remainDeps: []uint64{1}},
			},
		},
		{
			// Remove an ID that was never added — the DAG must be unchanged.
			name:      "no-op/non-existent id",
			nodes:     []nodeSpec{{1, nil}, {2, []uint64{1}}},
			removeIDs: []uint64{999},
			wantLen:   2,
			wantNodes: []wantNode{
				{id: 1, inDeg: 0, remainDeps: nil},
				{id: 2, inDeg: 1, remainDeps: []uint64{1}},
			},
		},
		{
			// 1 → 2 → 3
			// Remove root then middle: only the leaf survives with in-degree 0.
			name:      "linear/remove root then middle",
			nodes:     []nodeSpec{{1, nil}, {2, []uint64{1}}, {3, []uint64{2}}},
			removeIDs: []uint64{1, 2},
			wantLen:   1,
			wantNodes: []wantNode{
				{id: 3, inDeg: 0, remainDeps: nil},
			},
		},
		{
			// Diamond:  1 → 2
			//           1 → 3
			//           2 → 4
			//           3 → 4
			// Remove the shared root (1).
			// Both 2 and 3 lose their only dep; 4 still has in-degree 2.
			name: "diamond/remove shared root",
			nodes: []nodeSpec{
				{1, nil},
				{2, []uint64{1}},
				{3, []uint64{1}},
				{4, []uint64{2, 3}},
			},
			removeIDs: []uint64{1},
			wantLen:   3,
			wantNodes: []wantNode{
				{id: 2, inDeg: 0, remainDeps: nil},
				{id: 3, inDeg: 0, remainDeps: nil},
				{id: 4, inDeg: 2, remainDeps: []uint64{2, 3}},
			},
		},
		{
			// Diamond: remove the convergence leaf (4).
			// 1, 2, 3 are untouched.
			name: "diamond/remove convergence leaf",
			nodes: []nodeSpec{
				{1, nil},
				{2, []uint64{1}},
				{3, []uint64{1}},
				{4, []uint64{2, 3}},
			},
			removeIDs: []uint64{4},
			wantLen:   3,
			wantNodes: []wantNode{
				{id: 1, inDeg: 0, remainDeps: nil},
				{id: 2, inDeg: 1, remainDeps: []uint64{1}},
				{id: 3, inDeg: 1, remainDeps: []uint64{1}},
			},
		},
		{
			// Diamond: remove root then one branch (1, then 2).
			// After step 1: 2 and 3 have inDeg 0; 4 has inDeg 2.
			// After step 2: 3 still has inDeg 0; 4 loses dep 2, so inDeg drops to 1.
			name: "diamond/remove root then one branch",
			nodes: []nodeSpec{
				{1, nil},
				{2, []uint64{1}},
				{3, []uint64{1}},
				{4, []uint64{2, 3}},
			},
			removeIDs: []uint64{1, 2},
			wantLen:   2,
			wantNodes: []wantNode{
				{id: 3, inDeg: 0, remainDeps: nil},
				{id: 4, inDeg: 1, remainDeps: []uint64{3}},
			},
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			// Build the DAG from the spec.
			tasks := make(map[uint64]Task, len(tc.nodes))
			d := New()
			for _, ns := range tc.nodes {
				t := stask(ns.id, fmt.Sprintf("node-%d", ns.id))
				tasks[ns.id] = t
				deps := make([]Task, len(ns.depIDs))
				for i, did := range ns.depIDs {
					deps[i] = tasks[did]
				}
				s.Require().NoError(d.AddNode(&Node{Task: t, Deps: deps}))
			}

			// Apply removals sequentially.
			for _, rid := range tc.removeIDs {
				d.RemoveNode(rid)
			}

			// 1. Correct node count.
			s.Equal(tc.wantLen, d.Len(), "unexpected node count")

			inDeg := d.InDegrees()
			for _, wn := range tc.wantNodes {
				// 2. Removed IDs are gone from the node map.
				for _, rid := range tc.removeIDs {
					_, found := d.Node(rid)
					s.False(found, "removed id=%d should not be in node map", rid)
				}

				// 3. In-degree matches expectation.
				s.Equal(wn.inDeg, inDeg[wn.id],
					"id=%d wrong in-degree", wn.id)

				// 4. Surviving node's Deps slice matches expectation.
				n, found := d.Node(wn.id)
				s.Require().True(found, "expected node id=%d to be present", wn.id)
				actualDepIDs := make([]uint64, len(n.Deps))
				for i, dep := range n.Deps {
					actualDepIDs[i] = dep.ID()
				}
				s.ElementsMatch(wn.remainDeps, actualDepIDs,
					"id=%d unexpected Deps", wn.id)

				// 5. No Dep references any removed ID.
				removedSet := make(map[uint64]bool, len(tc.removeIDs))
				for _, rid := range tc.removeIDs {
					removedSet[rid] = true
				}
				for _, dep := range n.Deps {
					s.False(removedSet[dep.ID()],
						"id=%d Deps still references removed id=%d", wn.id, dep.ID())
				}
			}

			// 6. Sort succeeds — graph is still a valid DAG.
			sorted, err := d.Sort()
			s.Require().NoError(err, "Sort should succeed after removal")
			s.Len(sorted, tc.wantLen)
		})
	}
}

func (s *DAGSuite) TestInDegrees() {
	d := New()
	a := stask(1, "a")
	b := stask(2, "b")
	c := stask(3, "c")
	_ = d.AddNode(&Node{Task: a})
	_ = d.AddNode(&Node{Task: b, Deps: []Task{a}})
	_ = d.AddNode(&Node{Task: c, Deps: []Task{a, b}})

	deg := d.InDegrees()
	s.Equal(0, deg[a.ID()])
	s.Equal(1, deg[b.ID()])
	s.Equal(2, deg[c.ID()])
}

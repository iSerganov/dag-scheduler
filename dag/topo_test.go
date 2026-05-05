package dag

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type TopoSuite struct {
	suite.Suite
}

func TestTopoSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TopoSuite))
}

func (s *TopoSuite) TestSort_Linear() {
	d := New()
	a, b, c := stask(1, "a"), stask(2, "b"), stask(3, "c")
	_ = d.AddNode(&Node{Task: a})
	_ = d.AddNode(&Node{Task: b, Deps: []Task{a}})
	_ = d.AddNode(&Node{Task: c, Deps: []Task{b}})

	sorted, err := d.Sort()
	s.Require().NoError(err)
	s.Require().Len(sorted, 3)

	pos := posMap(sorted)
	s.Less(pos[1], pos[2], "a must come before b")
	s.Less(pos[2], pos[3], "b must come before c")
}

func (s *TopoSuite) TestSort_Diamond() {
	//     A
	//    / \
	//   B   C
	//    \ /
	//     D
	d := New()
	a, b, c, dd := stask(1, "a"), stask(2, "b"), stask(3, "c"), stask(4, "d")
	_ = d.AddNode(&Node{Task: a})
	_ = d.AddNode(&Node{Task: b, Deps: []Task{a}})
	_ = d.AddNode(&Node{Task: c, Deps: []Task{a}})
	_ = d.AddNode(&Node{Task: dd, Deps: []Task{b, c}})

	sorted, err := d.Sort()
	s.Require().NoError(err)

	pos := posMap(sorted)
	s.Less(pos[a.ID()], pos[b.ID()], "a before b")
	s.Less(pos[a.ID()], pos[c.ID()], "a before c")
	s.Less(pos[b.ID()], pos[dd.ID()], "b before d")
	s.Less(pos[c.ID()], pos[dd.ID()], "c before d")
}

func (s *TopoSuite) TestSort_Disconnected() {
	d := New()
	_ = d.AddNode(&Node{Task: stask(1, "a")})
	_ = d.AddNode(&Node{Task: stask(2, "b")})

	sorted, err := d.Sort()
	s.Require().NoError(err)
	s.Len(sorted, 2)
}

func (s *TopoSuite) TestSort_ErrCycle_SentinelReachable() {
	// Verify the ErrCycle sentinel is a non-nil error value.
	s.Error(ErrCycle, "ErrCycle must be a non-nil sentinel")
}

func (s *TopoSuite) TestSort_ErrCycle() {
	// The public AddNode API prevents cycles (deps must pre-exist). Inject a
	// back-edge directly into unexported fields to simulate a cycle: A → B and
	// B → A (i.e. B depends on A but we also add a reverse edge so A appears
	// to depend on B).
	d := New()
	a := stask(1, "a")
	b := stask(2, "b")
	_ = d.AddNode(&Node{Task: a})
	_ = d.AddNode(&Node{Task: b, Deps: []Task{a}})

	// Inject back-edge: make A appear to depend on B as well.
	d.mu.Lock()
	d.adj[b.ID()] = append(d.adj[b.ID()], a.ID())
	d.inDeg[a.ID()]++
	d.mu.Unlock()

	_, err := d.Sort()
	s.Require().Error(err)
	s.ErrorIs(err, ErrCycle)
}

func (s *TopoSuite) TestSort_ErrCycle_SelfLoop() {
	// Self-loop: a node whose successor list points back to itself.
	d := New()
	a := stask(3, "a")
	_ = d.AddNode(&Node{Task: a})

	d.mu.Lock()
	d.adj[a.ID()] = append(d.adj[a.ID()], a.ID())
	d.inDeg[a.ID()]++
	d.mu.Unlock()

	_, err := d.Sort()
	s.Require().Error(err)
	s.ErrorIs(err, ErrCycle)
}

func (s *TopoSuite) TestSortAndSnapshot_InDegAndDependents() {
	//   A
	//  / \
	// B   C
	//  \ /
	//   D
	d := New()
	a, b, c, dd := stask(1, "a"), stask(2, "b"), stask(3, "c"), stask(4, "d")
	_ = d.AddNode(&Node{Task: a})
	_ = d.AddNode(&Node{Task: b, Deps: []Task{a}})
	_ = d.AddNode(&Node{Task: c, Deps: []Task{a}})
	_ = d.AddNode(&Node{Task: dd, Deps: []Task{b, c}})

	sorted, inDeg, dependents, err := d.SortAndSnapshot()
	s.Require().NoError(err)
	s.Len(sorted, 4)

	// Topological order must be consistent.
	pos := posMap(sorted)
	s.Less(pos[a.ID()], pos[b.ID()])
	s.Less(pos[a.ID()], pos[c.ID()])
	s.Less(pos[b.ID()], pos[dd.ID()])
	s.Less(pos[c.ID()], pos[dd.ID()])

	// In-degrees must reflect the original graph (not mutated by Kahn).
	s.Equal(0, inDeg[a.ID()])
	s.Equal(1, inDeg[b.ID()])
	s.Equal(1, inDeg[c.ID()])
	s.Equal(2, inDeg[dd.ID()])

	// Dependents: A → {B, C}, B → {D}, C → {D}, D → {}.
	aDepIDs := nodeIDs(dependents[a.ID()])
	s.ElementsMatch([]uint64{b.ID(), c.ID()}, aDepIDs)
	s.Equal([]uint64{dd.ID()}, nodeIDs(dependents[b.ID()]))
	s.Equal([]uint64{dd.ID()}, nodeIDs(dependents[c.ID()]))
	s.Empty(dependents[dd.ID()])
}

func (s *TopoSuite) TestSortAndSnapshot_ErrCycle() {
	d := New()
	a := stask(1, "a")
	b := stask(2, "b")
	_ = d.AddNode(&Node{Task: a})
	_ = d.AddNode(&Node{Task: b, Deps: []Task{a}})

	d.mu.Lock()
	d.adj[b.ID()] = append(d.adj[b.ID()], a.ID())
	d.inDeg[a.ID()]++
	d.mu.Unlock()

	_, _, _, err := d.SortAndSnapshot()
	s.Require().Error(err)
	s.ErrorIs(err, ErrCycle)
}

// nodeIDs extracts Task.ID() from each node for compact assertion comparisons.
func nodeIDs(nodes []*Node) []uint64 {
	ids := make([]uint64, len(nodes))
	for i, n := range nodes {
		ids[i] = n.Task.ID()
	}

	return ids
}

// posMap returns a node-ID → position-in-slice map for assertion helpers.
func posMap(nodes []*Node) map[uint64]int {
	m := make(map[uint64]int, len(nodes))
	for i, n := range nodes {
		m[n.Task.ID()] = i
	}

	return m
}

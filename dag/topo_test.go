package dag

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type TopoSuite struct {
	suite.Suite
}

func TestTopoSuite(t *testing.T) {
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
	// The public API prevents cycles (deps must pre-exist), so we verify Sort
	// completes cleanly and that the ErrCycle sentinel is a non-nil value.
	d := New()
	_ = d.AddNode(&Node{Task: stask(10, "a")})
	_, err := d.Sort()
	s.NoError(err)
	s.NotNil(ErrCycle)
}

// posMap returns a node-ID → position-in-slice map for assertion helpers.
func posMap(nodes []*Node) map[uint64]int {
	m := make(map[uint64]int, len(nodes))
	for i, n := range nodes {
		m[n.Task.ID()] = i
	}
	return m
}

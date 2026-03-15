package dag

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

// stask creates a no-op Task with the given id and name. Defined here and
// shared with topo_test.go because both live in package dag.
func stask(id uint64, name string) Task {
	return Func(id, name, func(_ context.Context) error { return nil })
}

type DAGSuite struct {
	suite.Suite
}

func TestDAGSuite(t *testing.T) {
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

func (s *DAGSuite) TestRemoveNode_UpdatesDependentInDegree() {
	d := New()
	a := stask(1, "a")
	b := stask(2, "b")
	c := stask(3, "c")
	_ = d.AddNode(&Node{Task: a})
	_ = d.AddNode(&Node{Task: b, Deps: []Task{a}})
	_ = d.AddNode(&Node{Task: c, Deps: []Task{a, b}})

	d.RemoveNode(a.ID())

	s.Equal(2, d.Len(), "a should be gone")
	_, found := d.Node(a.ID())
	s.False(found, "Node(a) should return false")
	// b depended on a (1 dep), so its in-degree drops to 0.
	s.Equal(0, d.InDegrees()[b.ID()])
	// c depended on a and b (2 deps), so its in-degree drops to 1.
	s.Equal(1, d.InDegrees()[c.ID()])
}

func (s *DAGSuite) TestRemoveNode_NoOp_WhenMissing() {
	d := New()
	_ = d.AddNode(&Node{Task: stask(1, "a")})
	d.RemoveNode(999) // should not panic or change anything
	s.Equal(1, d.Len())
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

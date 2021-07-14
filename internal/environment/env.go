package environment

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/database"
	"github.com/genjidb/genji/internal/stringutil"
)

// A Param represents a parameter passed by the user to the statement.
type Param struct {
	// Name of the param
	Name string

	// Value is the parameter value.
	Value interface{}
}

// Environment contains information about the context in which
// the expression is evaluated.
type Environment struct {
	Params  []Param
	Vars    *document.FieldBuffer
	Doc     document.Document
	Catalog database.Catalog
	Tx      *database.Transaction

	Outer *Environment
}

func New(d document.Document, params ...Param) *Environment {
	env := Environment{
		Params: params,
		Doc:    d,
	}

	return &env
}

func (e *Environment) GetOuter() *Environment {
	return e.Outer
}

func (e *Environment) SetOuter(env *Environment) {
	e.Outer = env
}

func (e *Environment) Get(path document.Path) (v document.Value, ok bool) {
	if e.Vars != nil {
		v, err := path.GetValueFromDocument(e.Vars)
		if err == nil {
			return v, true
		}
	}

	if e.Outer != nil {
		return e.Outer.Get(path)
	}

	return
}

func (e *Environment) Set(name string, v document.Value) {
	if e.Vars == nil {
		e.Vars = document.NewFieldBuffer()
	}

	e.Vars.Set(document.Path{document.PathFragment{FieldName: name}}, v)
}

func (e *Environment) GetDocument() (document.Document, bool) {
	if e.Doc != nil {
		return e.Doc, true
	}

	if e.Outer != nil {
		return e.Outer.GetDocument()
	}

	return nil, false
}

func (e *Environment) SetDocument(d document.Document) {
	e.Doc = d
}

func (e *Environment) SetParams(params []Param) {
	e.Params = params
}

func (e *Environment) GetParamByName(name string) (v document.Value, err error) {
	if len(e.Params) == 0 {
		if e.Outer != nil {
			return e.Outer.GetParamByName(name)
		}
	}

	for _, nv := range e.Params {
		if nv.Name == name {
			return document.NewValue(nv.Value)
		}
	}

	return nil, stringutil.Errorf("param %s not found", name)
}

func (e *Environment) GetParamByIndex(pos int) (document.Value, error) {
	if len(e.Params) == 0 {
		if e.Outer != nil {
			return e.Outer.GetParamByIndex(pos)
		}
	}

	idx := int(pos - 1)
	if idx >= len(e.Params) {
		return nil, stringutil.Errorf("cannot find param number %d", pos)
	}

	return document.NewValue(e.Params[idx].Value)
}

func (e *Environment) GetTx() *database.Transaction {
	if e.Tx != nil {
		return e.Tx
	}

	if outer := e.GetOuter(); outer != nil {
		return outer.GetTx()
	}

	return nil
}

func (e *Environment) GetCatalog() database.Catalog {
	if e.Catalog != nil {
		return e.Catalog
	}
	if outer := e.GetOuter(); outer != nil {
		return outer.GetCatalog()
	}

	return nil
}

func (e *Environment) Clone() (*Environment, error) {
	var newEnv Environment

	newEnv.Params = e.Params
	newEnv.Tx = e.Tx
	newEnv.Catalog = e.Catalog

	if e.Doc != nil {
		fb := document.NewFieldBuffer()
		err := fb.Copy(e.Doc)
		if err != nil {
			return nil, err
		}

		newEnv.Doc = fb
	}

	if e.Vars != nil {
		fb := document.NewFieldBuffer()
		err := fb.Copy(e.Vars)
		if err != nil {
			return nil, err
		}

		newEnv.Vars = fb
	}

	if e.Outer != nil {
		newOuter, err := e.Outer.Clone()
		if err != nil {
			return nil, err
		}
		newEnv.Outer = newOuter
	}

	return &newEnv, nil
}

package app

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ctrl-research/seaglass/internal/k8s"
)

func TestPaletteDeprioritizesGenerators(t *testing.T) {
	resources := []k8s.Resource{
		{GVR: schema.GroupVersionResource{Group: "generators.external-secrets.io", Version: "v1alpha1", Resource: "passwords"}, Kind: "Password", Namespaced: true},
		{GVR: schema.GroupVersionResource{Group: "generators.external-secrets.io", Version: "v1alpha1", Resource: "fakegenerators"}, Kind: "FakeGenerator", Namespaced: true},
		{GVR: schema.GroupVersionResource{Group: "external-secrets.io", Version: "v1", Resource: "externalsecrets"}, Kind: "ExternalSecret", Namespaced: true},
	}
	p := newPalette()
	p.setItems(buildItems(resources, nil, nil, nil))
	p.show()
	for _, ch := range "external" {
		p.input.SetValue(p.input.Value() + string(ch))
	}
	p.filter()
	if len(p.matches) < 2 { t.Fatalf("expected matching resources, got %d", len(p.matches)) }
	first := p.items[p.matches[0]]
	if first.Resource.Kind != "ExternalSecret" {
		t.Fatalf("external should prefer ExternalSecret, got %s", first.Resource.Kind)
	}
	p.input.SetValue("generator")
	p.filter()
	if len(p.matches) == 0 { t.Fatal("generator resources should remain searchable") }
}

package app

import (
	"context"
	"sort"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/ctrl-research/seaglass/internal/k8s"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type serviceWorkloadsMsg struct {
	targets []relatedTarget
	err     error
}

func (m *Model) resolveServiceWorkloads(t relatedTarget) tea.Cmd {
	stream, get, resources := m.deps.stream, m.deps.get, append([]k8s.Resource(nil), m.resources...)
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		updates := stream.Stream(ctx, k8s.Pods, t.namespace, "", t.selector, true)
		var snapshot k8s.Snapshot
		select {
		case u, ok := <-updates:
			if !ok {
				return serviceWorkloadsMsg{}
			}
			if u.Err != nil {
				return serviceWorkloadsMsg{err: u.Err}
			}
			snapshot = u.Snapshot
		case <-ctx.Done():
			return serviceWorkloadsMsg{err: ctx.Err()}
		}
		seen := map[string]bool{}
		var targets []relatedTarget
		for _, row := range snapshot.Rows {
			pod := row.Object
			if pod == nil {
				var err error
				pod, err = get.Get(ctx, k8s.Pods, t.namespace, row.Name)
				if err != nil {
					continue
				}
			}
			owner, ok := k8s.ControllerOwner(pod)
			if !ok {
				continue
			}
			if owner.Kind == "ReplicaSet" {
				if rs, found := k8s.ResourceByKind(resources, "ReplicaSet", owner.APIVersion); found {
					if obj, err := get.Get(ctx, rs, t.namespace, owner.Name); err == nil {
						if parent, yes := k8s.ControllerOwner(obj); yes && parent.Kind == "Deployment" {
							owner = parent
						}
					}
				}
			}
			res, ok := k8s.ResourceByKind(resources, owner.Kind, owner.APIVersion)
			if !ok {
				continue
			}
			key := res.GVR.String() + "/" + t.namespace + "/" + owner.Name
			if seen[key] {
				continue
			}
			seen[key] = true
			targets = append(targets, relatedTarget{kind: relObject, label: res.Kind + "/" + owner.Name, detail: t.namespace,
				targetKind: res.Kind, targetGroup: res.GVR.Group, targetName: owner.Name, namespace: t.namespace, title: owner.Name})
		}
		sort.Slice(targets, func(i, j int) bool { return targets[i].label < targets[j].label })
		return serviceWorkloadsMsg{targets: targets}
	}
}

// service resolution does not guess a workload from the Service name.
func (m Model) handleServiceWorkloads(msg serviceWorkloadsMsg) (tea.Model, tea.Cmd) {
	if msg.err != nil {
		m.err = msg.err
		return m, nil
	}
	if len(msg.targets) == 0 {
		return m, m.setNotice("no workloads found for service selector")
	}
	return m.handleRelated(relatedMsg{targets: msg.targets})
}

var _ = unstructured.Unstructured{}

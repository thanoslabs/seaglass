package app

import (
	"context"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/ctrl-research/seaglass/internal/k8s"
)

type workloadLogsMsg struct {
	ns, title string
	targets   []logTarget
	resource  k8s.Resource
	selector  string
	err       error
}

// resolveWorkloadPods uses selectors (not resource names), preserving the
// namespace of the selected object even in an all-namespace table.
func (m *Model) resolveWorkloadPods(res k8s.Resource, ns, name string) tea.Cmd {
	get, stream := m.deps.get, m.deps.stream
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		obj, err := get.Get(ctx, res, ns, name)
		if err != nil {
			return workloadLogsMsg{err: err}
		}
		selector, ok := k8s.LabelSelectorString(obj)
		if !ok {
			return workloadLogsMsg{ns: ns, title: res.Kind + "/" + name}
		}
		updates := stream.Stream(ctx, k8s.Pods, ns, "", selector, true)
		var snap k8s.Snapshot
		select {
		case update, open := <-updates:
			if !open {
				return workloadLogsMsg{ns: ns, title: res.Kind + "/" + name}
			}
			if update.Err != nil {
				return workloadLogsMsg{err: update.Err}
			}
			snap = update.Snapshot
		case <-ctx.Done():
			return workloadLogsMsg{err: ctx.Err()}
		}
		var targets []logTarget
		for _, row := range snap.Rows {
			pod := row.Object
			if pod == nil {
				pod, err = get.Get(ctx, k8s.Pods, ns, row.Name)
				if err != nil {
					continue
				}
			}
			for _, container := range k8s.Containers(pod) {
				targets = append(targets, logTarget{pod: row.Name, container: container})
			}
		}
		return workloadLogsMsg{ns: ns, title: res.Kind + "/" + name, targets: targets, resource: res, selector: selector}
	}
}

func (m Model) handleWorkloadLogs(msg workloadLogsMsg) (tea.Model, tea.Cmd) {
	if msg.err != nil {
		m.err = msg.err
		return m, nil
	}
	if len(msg.targets) == 0 {
		return m, m.setNotice("no matching pod containers for logs")
	}
	m.top().stop()
	m.nextID++
	v := newWorkloadLogsView(m.nextID, msg.ns, msg.title, m.saveDir, msg.targets)
	m.stack = append(m.stack, v)
	cmd := m.startTop()
	watch := v.watchMembership(m.deps, msg.selector)
	return m, tea.Batch(cmd, watch)
}

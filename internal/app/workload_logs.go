package app

import (
	"context"
	"sort"
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

type membershipMsg struct {
	id      int
	targets []logTarget
	err     error
}

// watchMembership follows pod additions and removals for the lifetime of the log view.
func (v *logsView) watchMembership(d deps, selector string) tea.Cmd {
	ctx, cancel := context.WithCancel(context.Background())
	v.memberCancel = cancel
	updates := d.stream.Stream(ctx, k8s.Pods, v.namespace, "", selector, true)
	v.membershipUpdates = updates
	v.membershipCtx = ctx
	v.membershipGet = d.get
	id, get, ns := v.id, d.get, v.namespace
	return func() tea.Msg {
		for {
			select {
			case <-ctx.Done():
				return nil
			case update, ok := <-updates:
				if !ok {
					return nil
				}
				if update.Err != nil {
					return membershipMsg{id: id, err: update.Err}
				}
				if update.Status != k8s.StatusLive {
					continue
				}
				var targets []logTarget
				for _, row := range update.Snapshot.Rows {
					pod := row.Object
					if pod == nil {
						var err error
						pod, err = get.Get(ctx, k8s.Pods, ns, row.Name)
						if err != nil {
							continue
						}
					}
					for _, c := range k8s.Containers(pod) {
						targets = append(targets, logTarget{pod: row.Name, container: c})
					}
				}
				sort.Slice(targets, func(i, j int) bool {
					if targets[i].pod != targets[j].pod {
						return targets[i].pod < targets[j].pod
					}
					return targets[i].container < targets[j].container
				})
				return membershipMsg{id: id, targets: targets}
			}
		}
	}
}

func (v *logsView) nextMembership() tea.Cmd {
	ctx, updates, get, id, ns := v.membershipCtx, v.membershipUpdates, v.membershipGet, v.id, v.namespace
	if ctx == nil {
		return nil
	}
	return func() tea.Msg {
		for {
			select {
			case <-ctx.Done():
				return nil
			case update, ok := <-updates:
				if !ok {
					return nil
				}
				if update.Err != nil {
					return membershipMsg{id: id, err: update.Err}
				}
				if update.Status != k8s.StatusLive {
					continue
				}
				var targets []logTarget
				for _, row := range update.Snapshot.Rows {
					pod := row.Object
					if pod == nil {
						var err error
						pod, err = get.Get(ctx, k8s.Pods, ns, row.Name)
						if err != nil {
							continue
						}
					}
					for _, c := range k8s.Containers(pod) {
						targets = append(targets, logTarget{pod: row.Name, container: c})
					}
				}
				return membershipMsg{id: id, targets: targets}
			}
		}
	}
}

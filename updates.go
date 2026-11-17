package main

import openshell "github.com/ai-openshell/sdk-go/openshell/v1"

func applySourceUpdate(update sourceUpdate, views map[string]*openshell.ServiceRouteSnapshot, connected map[string]bool) {
	if update.revoke {
		delete(views, update.source)
	}
	if update.snapshot != nil {
		views[update.source] = update.snapshot
	}
	connected[update.source] = update.ready
}

// Every snapshot replaces its source's complete view. Consume updates that
// arrived during Kubernetes reconciliation before installing another view;
// otherwise queued renewals can expire while newer authority waits behind them.
// Keep FIFO revocations and their original deadlines, and bound this drain to
// the queue observed on entry so a busy source cannot starve reconciliation.
func drainSourceUpdates(updates <-chan sourceUpdate, views map[string]*openshell.ServiceRouteSnapshot, connected map[string]bool) {
	for range len(updates) {
		applySourceUpdate(<-updates, views, connected)
	}
}

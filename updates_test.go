package main

import (
	"testing"
	"time"

	openshell "github.com/ai-openshell/sdk-go/openshell/v1"
)

func TestBufferedRenewalsReconcileLatestUnexpiredInstallation(t *testing.T) {
	r, views := fixture()
	old := *views["source-a"]
	old.Routes = append([]openshell.DiscoveredServiceRoute(nil), old.Routes...)
	old.Routes[0].ExpiresAt = time.Now().Add(-time.Second)
	fresh := *views["source-a"]
	fresh.Routes = append([]openshell.DiscoveredServiceRoute(nil), fresh.Routes...)
	fresh.Revision = "renewed"
	fresh.Routes[0].Endpoint.Host = "10.0.0.2"
	fresh.Routes[0].InstallationVersion = "owner-2"
	expiry := fresh.Routes[0].ExpiresAt
	updates := make(chan sourceUpdate, 8)
	updates <- sourceUpdate{source: "source-a", snapshot: &old, ready: true}
	updates <- sourceUpdate{source: "source-a", snapshot: &fresh, ready: true}
	connected := map[string]bool{}
	drainSourceUpdates(updates, views, connected)
	mustReconcile(t, r, views)
	if objectCount(t, r, httpRoutes) != 1 || !connected["source-a"] {
		t.Fatal("a fresh complete view must retain the route")
	}
	installed := desiredInstallations(views, time.Now())
	if len(installed) != 1 || installed[0].route.Endpoint.Host != "10.0.0.2" || !installed[0].route.ExpiresAt.Equal(expiry) {
		t.Fatal("reconciliation must use the latest endpoint without extending its authority")
	}
}

func TestQueuedRevocationWithdrawsOnlyItsSource(t *testing.T) {
	r, views := fixture()
	other := *views["source-a"]
	other.Routes = append([]openshell.DiscoveredServiceRoute(nil), other.Routes...)
	other.Routes[0].Hostname = "other.example.com"
	views["source-b"] = &other
	mustReconcile(t, r, views)
	updates := make(chan sourceUpdate, 8)
	updates <- sourceUpdate{source: "source-a", snapshot: views["source-a"], ready: true}
	updates <- sourceUpdate{source: "source-a", revoke: true}
	connected := map[string]bool{"source-a": true, "source-b": true}
	drainSourceUpdates(updates, views, connected)
	mustReconcile(t, r, views)
	if views["source-a"] != nil || connected["source-a"] || !connected["source-b"] {
		t.Fatal("queued snapshots must not mask revocation or erase another source")
	}
	for _, gvr := range resources {
		if objectCount(t, r, gvr) != 1 {
			t.Fatal("only the other source's resources should remain")
		}
	}
}

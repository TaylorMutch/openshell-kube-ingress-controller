package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	openshell "github.com/ai-openshell/sdk-go/openshell/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Run against a seeded gateway that renews one installation and withdraws it
// after the marker is written. The local Rust validation fixture supplies it.
func TestServiceDiscoveryAgainstSeededRustGateway(t *testing.T) {
	endpoint := os.Getenv("OPENSHELL_DISCOVERY_VALIDATION_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires a seeded ServiceDiscovery validation gateway")
	}
	source := Source{ID: "source-a", Endpoint: endpoint, Workspace: "default", CAFile: os.Getenv("OPENSHELL_DISCOVERY_VALIDATION_CA"), TokenFile: os.Getenv("OPENSHELL_DISCOVERY_VALIDATION_TOKEN")}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	updates := make(chan sourceUpdate, 8)
	finished := make(chan error, 1)
	go func() { finished <- consumeSource(ctx, source, updates) }()
	r, views := fixture()
	clear(views)
	connected := map[string]bool{}
	var first *openshell.DiscoveredServiceRoute
	renewed := false
	for {
		select {
		case err := <-finished:
			t.Fatalf("discovery source ended before withdrawal: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case update := <-updates:
			if update.snapshot == nil {
				t.Fatal("missing replacement snapshot")
			}
			applySourceUpdate(update, views, connected)
			mustReconcile(t, r, views)
			if len(update.snapshot.Routes) == 0 {
				if !renewed {
					t.Fatal("withdrawal arrived before renewal")
				}
				for _, resource := range resources {
					if objectCount(t, r, resource) != 0 {
						t.Fatalf("withdrawal left %s", resource.Resource)
					}
				}
				cancel()
				select {
				case err := <-finished:
					if err != nil && !errors.Is(err, context.Canceled) && !openshell.IsCancelled(err) {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("discovery source did not cancel")
				}
				return
			}
			if len(update.snapshot.Routes) != 1 {
				t.Fatalf("unexpected routes: %v", update.snapshot.Routes)
			}
			route := update.snapshot.Routes[0]
			if strings.Contains(route.Hostname, "demo") || !strings.HasSuffix(route.Hostname, ".services.example.com") {
				t.Fatalf("expected authoritative hashed hostname, got %s", route.Hostname)
			}
			if route.Endpoint.Host != "10.0.0.7" || route.Endpoint.Port != 9443 {
				t.Fatalf("unexpected supervisor endpoint: %v", route.Endpoint)
			}
			for _, resource := range resources {
				if objectCount(t, r, resource) != 1 {
					t.Fatalf("missing %s", resource.Resource)
				}
			}
			item := desiredInstallations(views, time.Now())[0]
			name := objectName(r.config.ControllerID, item)
			httpRoute, err := r.client.Resource(httpRoutes).Namespace(r.config.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			hosts, _, _ := unstructured.NestedStringSlice(httpRoute.Object, "spec", "hostnames")
			if len(hosts) != 1 || hosts[0] != route.Hostname {
				t.Fatalf("HTTPRoute reconstructed hostname: %v", hosts)
			}
			policy, err := r.client.Resource(backendTLSPolicies).Namespace(r.config.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			sni, _, _ := unstructured.NestedString(policy.Object, "spec", "validation", "hostname")
			if sni != route.Hostname {
				t.Fatalf("backend SNI mismatch: %s", sni)
			}
			if first == nil {
				copy := route
				first = &copy
			} else if route.ExpiresAt.After(first.ExpiresAt) {
				if route.InstallationVersion != first.InstallationVersion {
					t.Fatal("renewal changed installation identity")
				}
				renewed = true
				if err := os.WriteFile(os.Getenv("OPENSHELL_DISCOVERY_VALIDATION_WITHDRAW"), []byte("withdraw\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

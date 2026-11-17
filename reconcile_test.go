package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	openshell "github.com/ai-openshell/sdk-go/openshell/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

func fixture() (*reconciler, map[string]*openshell.ServiceRouteSnapshot) {
	client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{services: "ServiceList", slices: "EndpointSliceList", httpRoutes: "HTTPRouteList", backendTLSPolicies: "BackendTLSPolicyList"})
	config := Config{ControllerID: "example", Namespace: "ingress", Gateway: "apps", GatewayNamespace: "ingress", Listener: "apps", BackendCAConfigMap: "supervisor-app-ca"}
	route := openshell.DiscoveredServiceRoute{Workspace: "default", Sandbox: "demo", Name: "web", Hostname: "demo.example.com", InstallationVersion: "owner-1", Endpoint: openshell.ServiceRouteEndpoint{Host: "10.0.0.1", Port: 9443}, ExpiresAt: time.Now().Add(10 * time.Second)}
	return &reconciler{config: config, client: client}, map[string]*openshell.ServiceRouteSnapshot{"source-a": {Revision: "revision-1", Routes: []openshell.DiscoveredServiceRoute{route}}}
}
func mustReconcile(t *testing.T, r *reconciler, views map[string]*openshell.ServiceRouteSnapshot) {
	t.Helper()
	if err := r.reconcile(context.Background(), views, time.Now()); err != nil {
		t.Fatal(err)
	}
}
func objectCount(t *testing.T, r *reconciler, gvr schema.GroupVersionResource) int {
	t.Helper()
	list, err := r.client.Resource(gvr).Namespace(r.config.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return len(list.Items)
}

func TestControllerCreatesRepairsAndReplacesBackendsWithoutGatewayOrProxy(t *testing.T) {
	r, views := fixture()
	mustReconcile(t, r, views)
	for _, gvr := range resources {
		if objectCount(t, r, gvr) != 1 {
			t.Fatalf("missing %s", gvr.Resource)
		}
	}
	item := desiredInstallations(views, time.Now())[0]
	name := objectName(r.config.ControllerID, item)
	service, err := r.client.Resource(services).Namespace("ingress").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(service.Object, "10.43.0.1", "spec", "clusterIP")
	if _, err = r.client.Resource(services).Namespace("ingress").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	views["source-a"].Routes[0].Endpoint.Host = "10.0.0.2"
	views["source-a"].Routes[0].InstallationVersion = "owner-2"
	mustReconcile(t, r, views)
	endpoints, err := r.client.Resource(slices).Namespace("ingress").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entries, _, _ := unstructured.NestedSlice(endpoints.Object, "endpoints")
	addresses := entries[0].(map[string]any)["addresses"].([]any)
	if addresses[0] != "10.0.0.2" {
		t.Fatalf("backend not replaced: %v", addresses)
	}
	service, err = r.client.Resource(services).Namespace("ingress").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ip, _, _ := unstructured.NestedString(service.Object, "spec", "clusterIP")
	if ip != "10.43.0.1" {
		t.Fatal("service allocation changed")
	}
	if err = r.client.Resource(httpRoutes).Namespace("ingress").Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	mustReconcile(t, r, views)
	if objectCount(t, r, httpRoutes) != 1 {
		t.Fatal("deleted route was not repaired")
	}
}

func TestHTTPRouteUsesDiscoveredHostnameAndValidatedBackendTLS(t *testing.T) {
	r, views := fixture()
	mustReconcile(t, r, views)
	item := desiredInstallations(views, time.Now())[0]
	name := objectName(r.config.ControllerID, item)
	policy, err := r.client.Resource(backendTLSPolicies).Namespace("ingress").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	hostname, _, _ := unstructured.NestedString(policy.Object, "spec", "validation", "hostname")
	refs, _, _ := unstructured.NestedSlice(policy.Object, "spec", "validation", "caCertificateRefs")
	targets, _, _ := unstructured.NestedSlice(policy.Object, "spec", "targetRefs")
	if hostname != item.route.Hostname || refs[0].(map[string]any)["name"] != r.config.BackendCAConfigMap || targets[0].(map[string]any)["name"] != name || targets[0].(map[string]any)["sectionName"] != "app" {
		t.Fatalf("incorrect backend TLS policy: %v", policy.Object)
	}
	route, err := r.client.Resource(httpRoutes).Namespace("ingress").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	hosts, _, _ := unstructured.NestedStringSlice(route.Object, "spec", "hostnames")
	rules, _, _ := unstructured.NestedSlice(route.Object, "spec", "rules")
	backends := rules[0].(map[string]any)["backendRefs"].([]any)
	if route.GetKind() != "HTTPRoute" || len(hosts) != 1 || hosts[0] != item.route.Hostname || backends[0].(map[string]any)["name"] != name {
		t.Fatalf("incorrect HTTPRoute: %v", route.Object)
	}
	// Repair policy drift and apply operator trust changes without adopting
	// discovery credentials as application trust or adding an insecure fallback.
	_ = unstructured.SetNestedField(policy.Object, "wrong.example.com", "spec", "validation", "hostname")
	if _, err = r.client.Resource(backendTLSPolicies).Namespace("ingress").Update(context.Background(), policy, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.config.BackendCAConfigMap = "rotated-supervisor-ca"
	mustReconcile(t, r, views)
	policy, err = r.client.Resource(backendTLSPolicies).Namespace("ingress").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	hostname, _, _ = unstructured.NestedString(policy.Object, "spec", "validation", "hostname")
	refs, _, _ = unstructured.NestedSlice(policy.Object, "spec", "validation", "caCertificateRefs")
	if hostname != item.route.Hostname || refs[0].(map[string]any)["name"] != "rotated-supervisor-ca" {
		t.Fatal("backend TLS policy was not repaired")
	}
}

func TestControllerRequiresExplicitBackendTrust(t *testing.T) {
	r, _ := fixture()
	r.config.Sources = []Source{{ID: "primary", Endpoint: "https://gateway.example.com", Workspace: "default"}}
	path := filepath.Join(t.TempDir(), "config.json")
	for _, valid := range []bool{false, true} {
		config := r.config
		if !valid {
			config.BackendCAConfigMap = ""
		}
		data, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		_, err = readConfig(path)
		if (err == nil) != valid {
			t.Fatalf("backend trust validation: valid=%v, err=%v", valid, err)
		}
	}
}

func TestControllerOrdersHTTPRouteAttachmentAndWithdrawalAroundBackendTLS(t *testing.T) {
	r, views := fixture()
	mustReconcile(t, r, views)
	mustReconcile(t, r, nil)
	created, deleted := []string{}, []string{}
	for _, action := range r.client.(*fake.FakeDynamicClient).Actions() {
		switch action.GetVerb() {
		case "create":
			created = append(created, action.GetResource().Resource)
		case "delete":
			deleted = append(deleted, action.GetResource().Resource)
		}
	}
	if !reflect.DeepEqual(created, []string{"services", "endpointslices", "backendtlspolicies", "httproutes"}) {
		t.Fatalf("route attached before backend mapping/TLS: %v", created)
	}
	if !reflect.DeepEqual(deleted, []string{"httproutes", "backendtlspolicies", "endpointslices", "services"}) {
		t.Fatalf("backend/TLS removed before route withdrawal: %v", deleted)
	}
}

func TestControllerExpiryWithdrawsOwnedResourcesAndLeavesForeignObjects(t *testing.T) {
	r, views := fixture()
	mustReconcile(t, r, views)
	foreign := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "foreign", "namespace": "ingress"}}}
	if _, err := r.client.Resource(services).Namespace("ingress").Create(context.Background(), foreign, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	views["source-a"].Routes[0].ExpiresAt = time.Now().Add(-time.Second)
	mustReconcile(t, r, views)
	if objectCount(t, r, httpRoutes) != 0 || objectCount(t, r, backendTLSPolicies) != 0 || objectCount(t, r, slices) != 0 || objectCount(t, r, services) != 1 {
		t.Fatal("expiry cleanup affected wrong resources")
	}
}

func TestControllerRejectsConflictingSourcesAndForeignNameCollisions(t *testing.T) {
	r, views := fixture()
	mustReconcile(t, r, views)
	duplicate := *views["source-a"]
	views["source-b"] = &duplicate
	if err := r.reconcile(context.Background(), views, time.Now()); err == nil {
		t.Fatal("hostname conflict not reported")
	}
	if objectCount(t, r, httpRoutes) != 0 {
		t.Fatal("ambiguous hostname retained routing")
	}
	delete(views, "source-b")
	item := desiredInstallations(views, time.Now())[0]
	foreign := r.objects(item)[services]
	foreign.SetLabels(map[string]string{ownerLabel: "other-controller"})
	if _, err := r.client.Resource(services).Namespace("ingress").Create(context.Background(), foreign, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcile(context.Background(), views, time.Now()); err == nil {
		t.Fatal("foreign resource adopted")
	}
	other := objectName("another-controller", item)
	if other == objectName(r.config.ControllerID, item) {
		t.Fatal("controller ownership naming not isolated")
	}
}

func TestControllerBoundsAndValidatesEntireSourceViews(t *testing.T) {
	_, views := fixture()
	view := views["source-a"]
	if !validSnapshot(view, "default") {
		t.Fatal("valid view rejected")
	}
	if validSnapshot(view, "other") {
		t.Fatal("cross-workspace view accepted")
	}
	view.Routes[0].Endpoint.Host = "127.0.0.1"
	if validSnapshot(view, "default") {
		t.Fatal("loopback backend accepted")
	}
	view.Routes[0].Endpoint.Host = "10.0.0.1"
	view.Routes[0].Hostname = "Bad.EXAMPLE"
	if validSnapshot(view, "default") {
		t.Fatal("invalid hostname accepted")
	}
	view.Routes = make([]openshell.DiscoveredServiceRoute, maxRoutes+1)
	if validSnapshot(view, "default") {
		t.Fatal("oversized view accepted")
	}
}

func TestControllerDoesNotPruneForgedOwnershipKeys(t *testing.T) {
	r, views := fixture()
	mustReconcile(t, r, views)
	item := desiredInstallations(views, time.Now())[0]
	forged := r.objects(item)[services]
	forged.SetName("forged-owner")
	if _, err := r.client.Resource(services).Namespace(r.config.Namespace).Create(context.Background(), forged, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcile(context.Background(), nil, time.Now()); err == nil {
		t.Fatal("forged ownership was not reported")
	}
	if objectCount(t, r, httpRoutes) != 0 || objectCount(t, r, backendTLSPolicies) != 0 || objectCount(t, r, slices) != 0 || objectCount(t, r, services) != 1 {
		t.Fatal("cleanup deleted a foreign key or retained valid owned orphans")
	}
}

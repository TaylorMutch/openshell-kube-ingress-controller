package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	openshell "github.com/ai-openshell/sdk-go/openshell/v1"
	"hash/fnv"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"net"
	"reflect"
	"sort"
	"time"
)

const ownerLabel = "openshell.ai/discovery-controller"
const sourceLabel = "openshell.ai/discovery-source"
const identityAnnotation = "openshell.ai/discovery-key"
const manager = "openshell-discovery-poc"

var services = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
var slices = schema.GroupVersionResource{Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"}
var httpRoutes = schema.GroupVersionResource{Group: "gateway.networking.k8s.io", Version: "v1", Resource: "httproutes"}
var backendTLSPolicies = schema.GroupVersionResource{Group: "gateway.networking.k8s.io", Version: "v1", Resource: "backendtlspolicies"}

// Withdraw the route before removing its TLS policy or backend mappings.
var resources = []schema.GroupVersionResource{httpRoutes, backendTLSPolicies, slices, services}

type installation struct {
	source string
	route  openshell.DiscoveredServiceRoute
}
type reconciler struct {
	config Config
	client dynamic.Interface
}

func desiredInstallations(views map[string]*openshell.ServiceRouteSnapshot, now time.Time) []installation {
	candidates := []installation{}
	counts := map[string]int{}
	for source, view := range views {
		if view == nil {
			continue
		}
		for _, route := range view.Routes {
			if route.ExpiresAt.After(now) {
				candidates = append(candidates, installation{source: source, route: route})
				counts[route.Hostname]++
			}
		}
	}
	desired := []installation{}
	for _, item := range candidates {
		if counts[item.route.Hostname] == 1 {
			desired = append(desired, item)
		}
	}
	sort.Slice(desired, func(i, j int) bool { return routeKey(desired[i]) < routeKey(desired[j]) })
	return desired
}
func routeKey(item installation) string {
	bytes, _ := json.Marshal([]string{item.source, item.route.Workspace, item.route.Sandbox, item.route.Name})
	return string(bytes)
}

// FNV is non-security resource naming; full keys fence every mutation.
func objectName(controller string, item installation) string {
	hash := fnv.New128a()
	_, _ = hash.Write([]byte(controller + "\x00" + routeKey(item)))
	return "os-route-" + hex.EncodeToString(hash.Sum(nil))
}
func ownsObject(object *unstructured.Unstructured, controller string) bool {
	if object.GetLabels()[ownerLabel] != controller || object.GetLabels()["app.kubernetes.io/managed-by"] != manager {
		return false
	}
	var key []string
	if json.Unmarshal([]byte(object.GetAnnotations()[identityAnnotation]), &key) != nil || len(key) != 4 || key[0] == "" {
		return false
	}
	item := installation{source: key[0], route: openshell.DiscoveredServiceRoute{Workspace: key[1], Sandbox: key[2], Name: key[3]}}
	return object.GetLabels()[sourceLabel] == key[0] && object.GetAnnotations()[identityAnnotation] == routeKey(item) && object.GetName() == objectName(controller, item)
}

func (r *reconciler) objects(item installation) map[schema.GroupVersionResource]*unstructured.Unstructured {
	name := objectName(r.config.ControllerID, item)
	metadata := func() map[string]any {
		return map[string]any{"name": name, "namespace": r.config.Namespace, "labels": map[string]any{ownerLabel: r.config.ControllerID, sourceLabel: item.source, "app.kubernetes.io/managed-by": manager}, "annotations": map[string]any{identityAnnotation: routeKey(item), "openshell.ai/service-hostname": item.route.Hostname, "openshell.ai/installation-version": item.route.InstallationVersion}}
	}
	addressType := "IPv6"
	if net.ParseIP(item.route.Endpoint.Host).To4() != nil {
		addressType = "IPv4"
	}
	service := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": metadata(), "spec": map[string]any{"type": "ClusterIP", "ports": []any{map[string]any{"name": "app", "protocol": "TCP", "port": int64(8443), "targetPort": int64(8443)}}}}}
	endpoints := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice", "metadata": metadata(), "addressType": addressType, "ports": []any{map[string]any{"name": "app", "protocol": "TCP", "port": int64(item.route.Endpoint.Port)}}, "endpoints": []any{map[string]any{"addresses": []any{item.route.Endpoint.Host}, "conditions": map[string]any{"ready": true}}}}}
	labels := endpoints.GetLabels()
	labels["kubernetes.io/service-name"] = name
	labels["endpointslice.kubernetes.io/managed-by"] = manager
	endpoints.SetLabels(labels)
	route := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute", "metadata": metadata(),
		"spec": map[string]any{
			"parentRefs": []any{map[string]any{"group": "gateway.networking.k8s.io", "kind": "Gateway", "name": r.config.Gateway, "namespace": r.config.GatewayNamespace, "sectionName": r.config.Listener}},
			"hostnames":  []any{item.route.Hostname},
			"rules": []any{map[string]any{
				"timeouts":    map[string]any{"request": "0s", "backendRequest": "0s"},
				"backendRefs": []any{map[string]any{"group": "", "kind": "Service", "name": name, "port": int64(8443), "weight": int64(1)}},
			}},
		},
	}}
	// The operator supplies backend CA trust independently of discovery identity
	// and the Gateway's public wildcard certificate. Never disable validation.
	policy := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "gateway.networking.k8s.io/v1", "kind": "BackendTLSPolicy", "metadata": metadata(),
		"spec": map[string]any{
			"targetRefs": []any{map[string]any{"group": "", "kind": "Service", "name": name, "sectionName": "app"}},
			"validation": map[string]any{
				"hostname":          item.route.Hostname,
				"caCertificateRefs": []any{map[string]any{"group": "", "kind": "ConfigMap", "name": r.config.BackendCAConfigMap}},
			},
		},
	}}
	return map[schema.GroupVersionResource]*unstructured.Unstructured{services: service, slices: endpoints, backendTLSPolicies: policy, httpRoutes: route}
}
func (r *reconciler) ensure(ctx context.Context, gvr schema.GroupVersionResource, wanted *unstructured.Unstructured) error {
	api := r.client.Resource(gvr).Namespace(r.config.Namespace)
	for attempt := 0; attempt < 3; attempt++ {
		current, err := api.Get(ctx, wanted.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = api.Create(ctx, wanted, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return err
		}
		if err != nil {
			return err
		}
		if !ownsObject(current, r.config.ControllerID) || current.GetAnnotations()[identityAnnotation] != wanted.GetAnnotations()[identityAnnotation] {
			return fmt.Errorf("refusing foreign resource %s/%s", gvr.Resource, wanted.GetName())
		}
		next := wanted.DeepCopy()
		if gvr == services {
			for _, key := range []string{"clusterIP", "clusterIPs", "ipFamilies", "ipFamilyPolicy", "sessionAffinity", "sessionAffinityConfig", "internalTrafficPolicy", "trafficDistribution"} {
				if value, found, _ := unstructured.NestedFieldCopy(current.Object, "spec", key); found {
					_ = unstructured.SetNestedField(next.Object, value, "spec", key)
				}
			}
		}
		labels := current.GetLabels()
		for key, value := range next.GetLabels() {
			labels[key] = value
		}
		next.SetLabels(labels)
		annotations := current.GetAnnotations()
		for key, value := range next.GetAnnotations() {
			annotations[key] = value
		}
		next.SetAnnotations(annotations)
		same := reflect.DeepEqual(current.GetLabels(), next.GetLabels()) && reflect.DeepEqual(current.GetAnnotations(), next.GetAnnotations())
		if gvr == slices {
			same = same && reflect.DeepEqual(current.Object["endpoints"], next.Object["endpoints"]) && reflect.DeepEqual(current.Object["ports"], next.Object["ports"]) && reflect.DeepEqual(current.Object["addressType"], next.Object["addressType"])
		} else {
			same = same && reflect.DeepEqual(current.Object["spec"], next.Object["spec"])
		}
		if same {
			return nil
		}
		next.SetResourceVersion(current.GetResourceVersion())
		next.SetUID(current.GetUID())
		_, err = api.Update(ctx, next, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			continue
		}
		return err
	}
	return fmt.Errorf("resource changed repeatedly during reconciliation")
}
func (r *reconciler) reconcile(ctx context.Context, views map[string]*openshell.ServiceRouteSnapshot, now time.Time) error {
	desired := desiredInstallations(views, now)
	unexpired := 0
	for _, view := range views {
		if view != nil {
			for _, route := range view.Routes {
				if route.ExpiresAt.After(now) {
					unexpired++
				}
			}
		}
	}
	conflicts := len(desired) < unexpired
	overflow := len(desired) > maxRoutes
	if overflow {
		desired = nil
	}
	names := map[string]bool{}
	for _, item := range desired {
		names[objectName(r.config.ControllerID, item)] = true
	}
	var ownershipError error
	for _, gvr := range resources {
		api := r.client.Resource(gvr).Namespace(r.config.Namespace)
		list, err := api.List(ctx, metav1.ListOptions{LabelSelector: ownerLabel + "=" + r.config.ControllerID})
		if err != nil {
			return err
		}
		for _, object := range list.Items {
			if names[object.GetName()] {
				continue
			}
			if !ownsObject(&object, r.config.ControllerID) {
				ownershipError = fmt.Errorf("refusing to prune resource with invalid ownership")
				continue
			}
			uid, version := object.GetUID(), object.GetResourceVersion()
			if err = api.Delete(ctx, object.GetName(), metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}}); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	if ownershipError != nil {
		return ownershipError
	}
	for _, item := range desired {
		objects := r.objects(item)
		for _, gvr := range []schema.GroupVersionResource{services, slices, backendTLSPolicies, httpRoutes} {
			if !item.route.ExpiresAt.After(time.Now()) {
				break
			}
			if err := r.ensure(ctx, gvr, objects[gvr]); err != nil {
				return err
			}
		}
	}
	if overflow {
		return fmt.Errorf("combined discovery view exceeds the POC route limit")
	}
	if conflicts {
		return fmt.Errorf("discovery sources claim conflicting hostnames")
	}
	return nil
}

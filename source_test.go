package main

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/ai-openshell/sdk-go/proto/openshellv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type discoveryEvent struct {
	view *pb.ServiceRouteSnapshot
	err  error
}

type discoveryFixture struct {
	pb.UnimplementedServiceDiscoveryServer
	first  *pb.ServiceRouteSnapshot
	events chan discoveryEvent
}

func discoveryAuthorized(ctx context.Context, workspace string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	token := md.Get("authorization")
	if workspace != "default" || len(token) != 1 || token[0] != "Bearer controller-test" {
		return status.Error(codes.PermissionDenied, "wrong discovery identity or workspace")
	}
	return nil
}

func (f *discoveryFixture) GetServiceRouteSnapshot(ctx context.Context, req *pb.GetServiceRouteSnapshotRequest) (*pb.GetServiceRouteSnapshotResponse, error) {
	if err := discoveryAuthorized(ctx, req.GetWorkspaceScope().GetWorkspace()); err != nil {
		return nil, err
	}
	return &pb.GetServiceRouteSnapshotResponse{Snapshot: f.first}, nil
}

func (f *discoveryFixture) WatchServiceRoutes(req *pb.WatchServiceRoutesRequest, stream grpc.ServerStreamingServer[pb.WatchServiceRoutesResponse]) error {
	if err := discoveryAuthorized(stream.Context(), req.GetWorkspaceScope().GetWorkspace()); err != nil {
		return err
	}
	if err := stream.Send(&pb.WatchServiceRoutesResponse{Snapshot: f.first}); err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case event := <-f.events:
			if event.err != nil {
				return event.err
			}
			if err := stream.Send(&pb.WatchServiceRoutesResponse{Snapshot: event.view}); err != nil {
				return err
			}
		}
	}
}

func startDiscoveryFixture(t *testing.T) (Source, *discoveryFixture) {
	t.Helper()
	certServer := httptest.NewTLSServer(nil)
	cert := certServer.TLS.Certificates[0]
	certServer.Close()
	caPath := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("controller-test\n"), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}})))
	f := &discoveryFixture{events: make(chan discoveryEvent, 4)}
	f.first = &pb.ServiceRouteSnapshot{Revision: "initial"}
	for i, identity := range [][2]string{{"sandbox-a", "web"}, {"sandbox-a", "api"}, {"sandbox-b", "web"}} {
		hosts := []string{"enh47uqyj7bpyttz.services.example.com", "abc234.services.example.com", "def567.services.example.com"}
		f.first.Routes = append(f.first.Routes, &pb.DiscoveredServiceRoute{
			Workspace: "default", Sandbox: identity[0], Name: identity[1], Hostname: hosts[i],
			Endpoint: &pb.ServiceRouteEndpoint{Host: "10.0.0.7", Port: 9443}, InstallationVersion: "owner-1",
			DeclarationRevision: 1, ExpiresAt: timestamppb.New(time.Now().Add(12 * time.Second)),
		})
	}
	// Deliberately register no OpenShell service: only the new API can work.
	pb.RegisterServiceDiscoveryServer(server, f)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return Source{ID: "source-a", Endpoint: "https://" + listener.Addr().String(), Workspace: "default", CAFile: caPath, TokenFile: tokenPath}, f
}

func receiveUpdate(t *testing.T, updates <-chan sourceUpdate) sourceUpdate {
	t.Helper()
	select {
	case update := <-updates:
		return update
	case <-time.After(5 * time.Second):
		t.Fatal("discovery update did not arrive")
		return sourceUpdate{}
	}
}

func TestSeparateDiscoveryServiceDrivesHTTPRoutesOverTLS(t *testing.T) {
	source, server := startDiscoveryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	updates := make(chan sourceUpdate, 8)
	done := make(chan error, 1)
	go func() { done <- consumeSource(ctx, source, updates) }()
	r, views := fixture()
	clear(views)
	connected := map[string]bool{}
	install := func(update sourceUpdate) {
		t.Helper()
		if update.snapshot == nil {
			t.Fatal("expected complete view")
		}
		applySourceUpdate(update, views, connected)
		mustReconcile(t, r, views)
		for _, resource := range resources {
			if got := objectCount(t, r, resource); got != len(update.snapshot.Routes) {
				t.Fatalf("%s count = %d", resource.Resource, got)
			}
		}
		for _, item := range desiredInstallations(views, time.Now()) {
			name := objectName(r.config.ControllerID, item)
			route, err := r.client.Resource(httpRoutes).Namespace(r.config.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			hosts, _, _ := unstructured.NestedStringSlice(route.Object, "spec", "hostnames")
			if len(hosts) != 1 || hosts[0] != item.route.Hostname {
				t.Fatal("hostname was reconstructed")
			}
			policy, err := r.client.Resource(backendTLSPolicies).Namespace(r.config.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			sni, _, _ := unstructured.NestedString(policy.Object, "spec", "validation", "hostname")
			if sni != item.route.Hostname {
				t.Fatal("backend TLS hostname differs from discovery")
			}
			slice, err := r.client.Resource(slices).Namespace(r.config.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			endpoints, _, _ := unstructured.NestedSlice(slice.Object, "endpoints")
			addresses := endpoints[0].(map[string]any)["addresses"].([]any)
			ports, _, _ := unstructured.NestedSlice(slice.Object, "ports")
			if addresses[0] != item.route.Endpoint.Host || ports[0].(map[string]any)["port"] != int64(item.route.Endpoint.Port) {
				t.Fatal("backend differs from supervisor endpoint")
			}
		}
	}
	install(receiveUpdate(t, updates)) // Unary snapshot.
	install(receiveUpdate(t, updates)) // Initial watch replacement.
	renewed := proto.Clone(server.first).(*pb.ServiceRouteSnapshot)
	renewed.Revision = "renewed"
	for _, route := range renewed.Routes {
		route.ExpiresAt = timestamppb.New(time.Now().Add(14 * time.Second))
	}
	server.events <- discoveryEvent{view: renewed}
	install(receiveUpdate(t, updates))
	replaced := proto.Clone(renewed).(*pb.ServiceRouteSnapshot)
	replaced.Revision = "owner-replaced"
	replaced.Routes[0].Endpoint.Host = "10.0.0.8"
	replaced.Routes[0].InstallationVersion = "owner-2"
	server.events <- discoveryEvent{view: replaced}
	install(receiveUpdate(t, updates))
	server.events <- discoveryEvent{view: &pb.ServiceRouteSnapshot{Revision: "withdrawn"}}
	install(receiveUpdate(t, updates))
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("discovery consumer did not stop")
	}
}

func TestDiscoveryAuthorizationLossWithdrawsSource(t *testing.T) {
	source, server := startDiscoveryFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := make(chan sourceUpdate, 8)
	done := make(chan struct{})
	go func() { defer close(done); watchSource(ctx, source, updates) }()
	r, views := fixture()
	connected := map[string]bool{}
	applySourceUpdate(receiveUpdate(t, updates), views, connected)
	applySourceUpdate(receiveUpdate(t, updates), views, connected)
	mustReconcile(t, r, views)
	server.events <- discoveryEvent{err: status.Error(codes.PermissionDenied, "grant revoked")}
	update := receiveUpdate(t, updates)
	if !update.revoke || update.ready {
		t.Fatal("authorization loss retained discovery authority")
	}
	applySourceUpdate(update, views, connected)
	mustReconcile(t, r, views)
	for _, resource := range resources {
		if objectCount(t, r, resource) != 0 {
			t.Fatal("revoked routes remain installed")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("discovery watcher did not stop")
	}
}

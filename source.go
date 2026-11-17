package main

import (
	"context"
	"errors"
	openshell "github.com/ai-openshell/sdk-go/openshell/v1"
	"io"
	"k8s.io/apimachinery/pkg/util/validation"
	"log/slog"
	"net"
	"time"
)

const maxRoutes = 64

type sourceUpdate struct {
	source   string
	snapshot *openshell.ServiceRouteSnapshot
	revoke   bool
	ready    bool
}

func validSnapshot(snapshot *openshell.ServiceRouteSnapshot, workspace string) bool {
	if snapshot == nil || len(snapshot.Routes) > maxRoutes || snapshot.Revision == "" {
		return false
	}
	seen := map[string]bool{}
	now := time.Now()
	for index, route := range snapshot.Routes {
		ip := net.ParseIP(route.Endpoint.Host)
		if route.Workspace != workspace || ip == nil || ip.IsLoopback() || ip.IsUnspecified() || ip.IsMulticast() || route.Endpoint.Port == 0 || route.Endpoint.Port > 65535 || route.Hostname == "" || len(validation.IsDNS1123Subdomain(route.Hostname)) != 0 || route.InstallationVersion == "" || route.ExpiresAt.IsZero() || seen[route.Hostname] {
			return false
		}
		seen[route.Hostname] = true
		snapshot.Routes[index].ExpiresAt = now.Add(min(route.ExpiresAt.Sub(now), 15*time.Second))
	}
	return true
}
func sendUpdate(ctx context.Context, updates chan<- sourceUpdate, value sourceUpdate) bool {
	select {
	case updates <- value:
		return true
	case <-ctx.Done():
		return false
	}
}
func consumeSource(ctx context.Context, source Source, updates chan<- sourceUpdate) error {
	client, err := source.client()
	if err != nil {
		return err
	}
	defer client.Close()
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	snapshot, err := client.ServiceRoutes().Snapshot(requestCtx, source.Workspace)
	cancel()
	if err != nil {
		return err
	}
	if !validSnapshot(snapshot, source.Workspace) {
		sendUpdate(ctx, updates, sourceUpdate{source: source.ID, revoke: true})
		return errors.New("invalid discovery view")
	}
	if !sendUpdate(ctx, updates, sourceUpdate{source: source.ID, snapshot: snapshot, ready: true}) {
		return ctx.Err()
	}
	watchCtx, watchCancel := context.WithTimeout(ctx, 70*time.Second)
	defer watchCancel()
	watch, err := client.ServiceRoutes().Watch(watchCtx, source.Workspace)
	if err != nil {
		return err
	}
	defer watch.Close()
	for {
		snapshot, err = watch.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if !validSnapshot(snapshot, source.Workspace) {
			sendUpdate(ctx, updates, sourceUpdate{source: source.ID, revoke: true})
			return errors.New("invalid discovery view")
		}
		if !sendUpdate(ctx, updates, sourceUpdate{source: source.ID, snapshot: snapshot, ready: true}) {
			return ctx.Err()
		}
	}
}
func watchSource(ctx context.Context, source Source, updates chan<- sourceUpdate) {
	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := consumeSource(ctx, source, updates)
		var statusError *openshell.StatusError
		integrityError := errors.As(err, &statusError) && statusError.Code == openshell.ErrorInternal
		revoked := integrityError || openshell.IsPermissionDenied(err) || openshell.IsUnauthenticated(err) || openshell.IsInvalidArgument(err) || openshell.IsConflict(err)
		if !sendUpdate(ctx, updates, sourceUpdate{source: source.ID, revoke: revoked}) {
			return
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("discovery reconnect required", "source", source.ID)
		}
		if time.Since(started) > 10*time.Second {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if err != nil {
			backoff = min(backoff*2, 30*time.Second)
		}
	}
}

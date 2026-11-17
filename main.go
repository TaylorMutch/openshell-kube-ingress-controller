package main

import (
	"context"
	"flag"
	"fmt"
	openshell "github.com/ai-openshell/sdk-go/openshell/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	coordination "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

func runController(ctx context.Context, config Config, client dynamic.Interface, ready *atomic.Bool) {
	updates := make(chan sourceUpdate, 8)
	for _, source := range config.Sources {
		go watchSource(ctx, source, updates)
	}
	views := map[string]*openshell.ServiceRouteSnapshot{}
	connected := map[string]bool{}
	reconcile := reconciler{config: config, client: client}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			ready.Store(false)
			return
		case update := <-updates:
			applySourceUpdate(update, views, connected)
		case <-ticker.C:
		}
		drainSourceUpdates(updates, views, connected)
		requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := reconcile.reconcile(requestCtx, views, time.Now())
		cancel()
		healthy := err == nil && len(connected) == len(config.Sources)
		for _, value := range connected {
			healthy = healthy && value
		}
		ready.Store(healthy)
		if err != nil && ctx.Err() == nil {
			slog.Warn("reconciliation failed", "error", err)
		}
	}
}
func main() {
	configPath := flag.String("config", "", "JSON deployment configuration")
	healthAddress := flag.String("health-address", "0.0.0.0:9092", "health listener")
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig path; omitted uses in-cluster identity")
	flag.Parse()
	config, err := readConfig(*configPath)
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	var kube *rest.Config
	if *kubeconfig != "" {
		kube, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	} else {
		kube, err = rest.InClusterConfig()
	}
	if err != nil {
		slog.Error("Kubernetes configuration failed", "error", err)
		os.Exit(1)
	}
	kube.Timeout = 10 * time.Second
	kube.QPS = 50
	kube.Burst = 100
	client, err := dynamic.NewForConfig(kube)
	if err != nil {
		panic(err)
	}
	leases, err := coordination.NewForConfig(kube)
	if err != nil {
		panic(err)
	}
	host, _ := os.Hostname()
	identity := fmt.Sprintf("%s-%d", host, time.Now().UnixNano())
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	var ready, leading atomic.Bool
	listener, err := net.Listen("tcp", *healthAddress)
	if err != nil {
		slog.Error("health listener failed", "error", err)
		os.Exit(1)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if leading.Load() && !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	healthServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 8192}
	go func() {
		if err := healthServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("health listener stopped")
			cancel()
		}
	}()
	defer healthServer.Close()
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          &resourcelock.LeaseLock{LeaseMeta: metav1.ObjectMeta{Name: config.ControllerID, Namespace: config.Namespace}, Client: leases, LockConfig: resourcelock.ResourceLockConfig{Identity: identity}},
		LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second, ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) { leading.Store(true); runController(leaderCtx, config, client, &ready) },
			OnStoppedLeading: func() { ready.Store(false); cancel() },
		},
	})
	if err != nil {
		slog.Error("leader election configuration failed", "error", err)
		os.Exit(1)
	}
	elector.Run(ctx)
}

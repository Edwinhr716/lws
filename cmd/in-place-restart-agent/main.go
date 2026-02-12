/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"os"
	"strconv"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	leaderworkerset "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	// We don't strictly need LWS scheme if we just watch Pods?
	// But good to have.
	utilruntime.Must(leaderworkerset.AddToScheme(scheme))
}

func main() {
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zap.Options{
		Development: true,
	})))

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		setupLog.Error(nil, "POD_NAME environment variable must be set")
		os.Exit(1)
	}
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		// Fallback to reading from file if needed, but for now expect env.
		// Standard downward API usage.
		namespaceBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err == nil {
			namespace = string(namespaceBytes)
		} else {
			setupLog.Error(nil, "POD_NAMESPACE environment variable must be set or service account namespace file readable")
			os.Exit(1)
		}
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		// We only care about THIS pod.
		// But in controller-runtime it's hard to watch just one object without filtering.
		// We will rely on caching everything in namespace or similar, or just use client directly?
		// A full manager for a sidecar might be heavy but it's robust.
		// We disable leader election.
		LeaderElection: false,
		Metrics:        ctrl.Options{}.Metrics, // Default? or disable.
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	agent := &InPlaceRestartAgent{
		Client:    mgr.GetClient(),
		PodName:   podName,
		Namespace: namespace,
		Log:       ctrl.Log.WithName("agent"),
	}

	if err = agent.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

type InPlaceRestartAgent struct {
	client.Client
	PodName   string
	Namespace string
	Log       logr.Logger
}

// Reconcile handles the in-place restart logic.
func (r *InPlaceRestartAgent) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Only reconcile OUR pod.
	if req.Name != r.PodName || req.Namespace != r.Namespace {
		return ctrl.Result{}, nil
	}

	log := r.Log.WithValues("pod", req.NamespacedName)
	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Read annotations
	currentAttemptStr := pod.Annotations[leaderworkerset.RestartAttemptAnnotationKey]
	pendingAttemptStr := pod.Annotations[leaderworkerset.PendingRestartAttemptAnnotationKey]

	currentAttempt := 0
	if currentAttemptStr != "" {
		currentAttempt, _ = strconv.Atoi(currentAttemptStr)
	}

	pendingAttempt := 0
	if pendingAttemptStr != "" {
		pendingAttempt, _ = strconv.Atoi(pendingAttemptStr)
	} else {
		// Initialize pending attempt.
		// If current is set, we might be joining late or starting fresh.
		// If we are starting fresh, we should set pending = 0?
		// Or if we assume we are fresh, we are at "current" if current is 0.
		// If current > 0, and we have no pending, we are "new" to the group BUT we might need to catch up?
		// Actually, if we are starting fresh (container start), we are implicitly running the latest version logic.
		// So we can say pending = current.
		// BUT if we restart because of in-place restart, we want pending to persist?
		// Annotations persist.
		// So if pending is missing, it is TRULY missing (new pod or first start).
		// We set pending = current.
		log.Info("Initializing pending restart attempt", "attempt", currentAttempt)
		err := r.patchPendingAttempt(ctx, &pod, currentAttempt)
		return ctrl.Result{}, err
	}

	if pendingAttempt < currentAttempt {
		log.Info("Pending restart attempt is less than current, restarting", "pending", pendingAttempt, "current", currentAttempt)
		// We need to catch up.
		// 1. Update pending to current (ACK).
		if err := r.patchPendingAttempt(ctx, &pod, currentAttempt); err != nil {
			return ctrl.Result{}, err
		}
		// 2. Exit to trigger restart.
		// We exit with a specific code? Or just 0/1?
		// JobSet uses 128+SIGTERM.
		// We can just use os.Exit(1).
		// But we need to make sure we don't loop tightly if restart fails or something?
		// Kubelet will restart us with exponential backoff.
		log.Info("Exiting to trigger restart")
		os.Exit(1)
	}

	// If pending == current, we are good.
	// If pending > current, that's weird but fine (we are ahead? or current rolled back?).

	return ctrl.Result{}, nil
}

func (r *InPlaceRestartAgent) patchPendingAttempt(ctx context.Context, pod *corev1.Pod, attempt int) error {
	patch := client.MergeFrom(pod.DeepCopy())
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[leaderworkerset.PendingRestartAttemptAnnotationKey] = strconv.Itoa(attempt)
	return r.Patch(ctx, pod, patch)
}

func (r *InPlaceRestartAgent) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(r)
}

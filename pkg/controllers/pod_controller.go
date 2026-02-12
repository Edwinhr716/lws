/*
Copyright 2023.

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

package controllers

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	appsapplyv1 "k8s.io/client-go/applyconfigurations/apps/v1"
	coreapplyv1 "k8s.io/client-go/applyconfigurations/core/v1"
	metaapplyv1 "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	leaderworkerset "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/pkg/schedulerprovider"
	acceleratorutils "sigs.k8s.io/lws/pkg/utils/accelerators"
	controllerutils "sigs.k8s.io/lws/pkg/utils/controller"
	podutils "sigs.k8s.io/lws/pkg/utils/pod"
	revisionutils "sigs.k8s.io/lws/pkg/utils/revision"
	statefulsetutils "sigs.k8s.io/lws/pkg/utils/statefulset"
)

// PodReconciler reconciles a LeaderWorkerSet object
type PodReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Record            record.EventRecorder
	SchedulerProvider schedulerprovider.SchedulerProvider
}

func NewPodReconciler(client client.Client, schema *runtime.Scheme, record record.EventRecorder, sp schedulerprovider.SchedulerProvider) *PodReconciler {
	return &PodReconciler{Client: client, Scheme: schema, Record: record, SchedulerProvider: sp}
}

//+kubebuilder:rbac:groups="",resources=events,verbs=create;watch;update;patch
//+kubebuilder:rbac:groups=core,resources=pods,verbs=create;delete;get;list;patch;update;watch
//+kubebuilder:rbac:groups=core,resources=pods/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;update;patch

func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log := ctrl.LoggerFrom(ctx).WithValues("pod", klog.KObj(&pod))
	ctx = ctrl.LoggerInto(ctx, log)

	// get the leaderWorkerSet name
	lwsName := pod.Labels[leaderworkerset.SetNameLabelKey]
	if lwsName == "" {
		return ctrl.Result{}, errors.New("leaderworkerset.sigs.k8s.io/name label is unexpected missing")
	}
	if _, exist := pod.Labels[leaderworkerset.WorkerIndexLabelKey]; !exist {
		return ctrl.Result{}, errors.New("leaderworkerset.sigs.k8s.io/worker-index label is unexpected missing")
	}
	// get the leaderWorkerSet object
	var leaderWorkerSet leaderworkerset.LeaderWorkerSet
	if err := r.Get(ctx, types.NamespacedName{Name: lwsName, Namespace: pod.Namespace}, &leaderWorkerSet); err != nil {
		// If lws not found, it's mostly because deleted, ignore the error as Pods will be GCed finally.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	leaderDeleted, err := r.handleRestartPolicy(ctx, pod, leaderWorkerSet)
	if err != nil {
		return ctrl.Result{}, err
	}
	if leaderDeleted {
		return ctrl.Result{}, nil
	}

	// worker pods' reconciliation is only done to handle restart policy
	if !podutils.LeaderPod(pod) {
		return ctrl.Result{}, nil
	}

	// validate leader's annotations to prevent infinite StatefulSet creation loops
	// see issue: https://github.com/kubernetes-sigs/lws/issues/391
	if pod.Annotations[leaderworkerset.LeaderPodNameAnnotationKey] != "" {
		errMsg := fmt.Sprintf("leader pod %s/%s contains mistake annotation '%s': requires Kubernetes ≥v1.27 or v1.26 with StatefulSetStartOrdinal feature",
			pod.Namespace,
			pod.Name,
			leaderworkerset.LeaderPodNameAnnotationKey)
		log.Error(errors.New(errMsg), "validate leader's annotations")
		r.Record.Eventf(&leaderWorkerSet, corev1.EventTypeWarning, FailedCreate, errMsg)
		return ctrl.Result{}, nil
	}

	if leaderWorkerSet.Spec.NetworkConfig != nil && *leaderWorkerSet.Spec.NetworkConfig.SubdomainPolicy == leaderworkerset.SubdomainUniquePerReplica {
		if err := controllerutils.CreateHeadlessServiceIfNotExists(ctx, r.Client, r.Scheme, &leaderWorkerSet, pod.Name, map[string]string{leaderworkerset.SetNameLabelKey: leaderWorkerSet.Name, leaderworkerset.GroupIndexLabelKey: pod.Labels[leaderworkerset.GroupIndexLabelKey]}, &pod); err != nil {
			return ctrl.Result{}, err
		}
	}

	// if it's not leader pod or leader pod is being deleted, we should not create the worker statefulset
	// this is critical to avoid race condition in all-or-nothing restart where the worker sts may be created
	// when the leader pod is being deleted
	if pod.DeletionTimestamp != nil {
		log.V(2).Info("skip creating the worker sts since the leader pod is being deleted")
		return ctrl.Result{}, nil
	}

	if r.SchedulerProvider != nil {
		err = r.SchedulerProvider.CreatePodGroupIfNotExists(ctx, &leaderWorkerSet, &pod)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// Once size = 1, no need to create worker statefulSets.
	if *leaderWorkerSet.Spec.LeaderWorkerTemplate.Size == 1 {
		return ctrl.Result{}, nil
	}

	// logic for handling leader pod
	if leaderWorkerSet.Spec.StartupPolicy == leaderworkerset.LeaderReadyStartupPolicy && !podutils.IsPodReady(&pod) {
		log.V(2).Info("defer the creation of the worker statefulset because leader pod is not ready.")
		return ctrl.Result{}, nil
	}
	revision, err := revisionutils.GetRevision(ctx, r.Client, &leaderWorkerSet, revisionutils.GetRevisionKey(&pod))
	if err != nil {
		log.Error(err, "Getting lws revisions")
		return ctrl.Result{}, err
	}
	if revision == nil {
		log.V(2).Info(fmt.Sprintf("Revision has not been created yet, requeing reconciler for pod %s", pod.Name))
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second}, nil
	}
	statefulSet, err := constructWorkerStatefulSetApplyConfiguration(pod, leaderWorkerSet, revision)
	if err != nil {
		return ctrl.Result{}, err
	}

	// if exclusive placement is enabled but leader pod is not scheduled, don't create the worker sts
	if topologyKey, found := leaderWorkerSet.Annotations[leaderworkerset.ExclusiveKeyAnnotationKey]; found {
		// check if the leader pod is scheduled.
		if pod.Spec.NodeName == "" {
			log.V(2).Info(fmt.Sprintf("Pod %q is not scheduled yet", pod.Name))
			return ctrl.Result{}, nil
		}
		if err := r.setNodeSelectorForWorkerPods(ctx, &pod, statefulSet, topologyKey); err != nil {
			log.Error(err, "setting node selector for worker pods")
			return ctrl.Result{}, err
		}
	}

	if err := setControllerReferenceWithStatefulSet(&pod, statefulSet, r.Scheme); err != nil {
		log.Error(err, "Setting controller reference.")
		return ctrl.Result{}, nil
	}

	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(statefulSet)
	if err != nil {
		return ctrl.Result{}, err
	}
	workerStatefulSet := &unstructured.Unstructured{
		Object: obj,
	}

	var workerSts appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: leaderWorkerSet.Namespace}, &workerSts); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		if err = r.Create(ctx, workerStatefulSet); err != nil {
			r.Record.Eventf(&leaderWorkerSet, corev1.EventTypeWarning, FailedCreate, fmt.Sprintf("Failed to create worker statefulset for leader pod %s", pod.Name))
			return ctrl.Result{}, client.IgnoreAlreadyExists(err)
		}
		r.Record.Eventf(&leaderWorkerSet, corev1.EventTypeNormal, GroupsProgressing, fmt.Sprintf("Created worker statefulset for leader pod %s", pod.Name))
	}
	log.V(2).Info("Worker Reconcile completed.")
	return ctrl.Result{}, nil
}

func (r *PodReconciler) handleRestartPolicy(ctx context.Context, pod corev1.Pod, leaderWorkerSet leaderworkerset.LeaderWorkerSet) (bool, error) {
	log := ctrl.LoggerFrom(ctx)
	if leaderWorkerSet.Spec.LeaderWorkerTemplate.RestartPolicy == leaderworkerset.RecreateGroupInPlace {
		return r.handleInPlaceRestart(ctx, pod, leaderWorkerSet)
	}

	if leaderWorkerSet.Spec.LeaderWorkerTemplate.RestartPolicy != leaderworkerset.RecreateGroupOnPodRestart {
		return false, nil
	}
	// the leader pod will be deleted if the worker pod is deleted or any container was restarted
	if !podutils.ContainerRestarted(pod) && !podutils.PodDeleted(pod) {
		return false, nil
	}

	pendingPods, err := r.pendingPodsInGroup(ctx, pod, int(*leaderWorkerSet.Spec.LeaderWorkerTemplate.Size))
	if err != nil {
		return false, err
	}

	_, recreateGroupAfterStart := leaderWorkerSet.Annotations[leaderworkerset.RecreateGroupAfterStart]

	if pendingPods && recreateGroupAfterStart {
		log.V(2).Info("Skipping RecreateGroupOnPodRestart because there is a pod pending: %s", pod.Name)
		return false, nil
	}

	var leader corev1.Pod
	if !podutils.LeaderPod(pod) {
		leaderPodName, ordinal := statefulsetutils.GetParentNameAndOrdinal(pod.Name)
		if ordinal == -1 {
			return false, fmt.Errorf("parsing pod name for pod %s", pod.Name)
		}
		if err := r.Get(ctx, types.NamespacedName{Name: leaderPodName, Namespace: pod.Namespace}, &leader); err != nil {
			// If the error is not found, it is likely caused by the fact that the leader was deleted but the worker statefulset
			// deletion hasn't deleted all the worker pods
			return false, client.IgnoreNotFound(err)
		}
		// Different revision key means that this pod will be deleted soon and alternative will be created with the matching key
		if revisionutils.GetRevisionKey(&leader) != revisionutils.GetRevisionKey(&pod) {
			return false, nil
		}
	} else {
		leader = pod
	}
	// if the leader pod is being deleted, we don't need to send deletion requests
	if leader.DeletionTimestamp != nil {
		return true, nil
	}
	deletionOpt := metav1.DeletePropagationForeground
	if err := r.Delete(ctx, &leader, &client.DeleteOptions{
		PropagationPolicy: &deletionOpt,
	}); err != nil {
		return false, err
	}
	r.Record.Eventf(&leaderWorkerSet, corev1.EventTypeNormal, "RecreateGroupOnPodRestart", fmt.Sprintf("Worker pod %s failed, deleted leader pod %s to recreate group %s", pod.Name, leader.Name, leader.Labels[leaderworkerset.GroupIndexLabelKey]))
	return true, nil
}

func (r *PodReconciler) handleInPlaceRestart(ctx context.Context, pod corev1.Pod, lws leaderworkerset.LeaderWorkerSet) (bool, error) {
	log := ctrl.LoggerFrom(ctx)

	// Check if any container in the pod has restarted or failed.
	// We only trigger restart if we see a failure/restart.
	// If the pod is simply Running and healthy, we do nothing.
	if !podutils.ContainerRestarted(pod) && !podInFailedState(pod) {
		return false, nil
	}

	// Fetch all pods in the group to determine if any of them have failed.
	// Actually, strictly speaking, this function is called per-pod.
	// But we need to coordinate the group.
	// If *this* pod is failed, we should trigger a group restart.
	// AND we need to check if the group is *already* in the middle of a restart?
	// The Agent logic: "Wait at barrier".
	// The Controller logic: "Increment restart attempt".

	// 1. Get the Leader Pod (to read current annotations).
	var leader corev1.Pod
	if podutils.LeaderPod(pod) {
		leader = pod
	} else {
		leaderName, _ := statefulsetutils.GetParentNameAndOrdinal(pod.Name)
		if err := r.Get(ctx, types.NamespacedName{Name: leaderName, Namespace: pod.Namespace}, &leader); err != nil {
			return false, client.IgnoreNotFound(err)
		}
	}

	// 2. Read annotations
	currentAttemptStr := leader.Annotations[leaderworkerset.RestartAttemptAnnotationKey]
	currentAttempt := 0
	if currentAttemptStr != "" {
		var err error
		currentAttempt, err = strconv.Atoi(currentAttemptStr)
		if err != nil {
			log.Error(err, "invalid restart attempt annotation", "value", currentAttemptStr)
			// Reset to 0? Or fail? Fail for now.
			return false, err
		}
	}

	// 3. Determine if this failure is "new".
	// If the pod has *already* acknowledged this attempt, then it might be failing *again*?
	// The Agent writes "pending-restart-attempt" (PodInPlaceRestartAttempt).
	// If Agent says "I am at attempt X", and X < Current, it is restarting.
	// If X == Current, it is running.
	// If we see a failure AND (X == Current OR X is missing), then it is a NEW failure.
	// If X < Current, it's already restarting, so ignoring it might be okay (or we might need to bump again if it failed *during* restart?).
	// JobSet logic: "If any job fails... and restarts < maxRestarts... recreate all".
	// For InPlace: "If any pod fails... increment attempt".
	
	// We need to check if we recently bumped it.
	// Ideally we check if the failure timestamp > last restart timestamp.
	// BUT we are using integer attempts.
	
	// Simple logic:
	// If Pod is failed/restarted, AND we haven't already bumped for this specific failure?
	// How do we know?
	// Maybe we rely on the Agent's "Pending" annotation?
	// If `pod-pending-restart-attempt` == `current-restart-attempt`, AND it is failed -> Bump.
	// If `pod-pending-restart-attempt` < `current-restart-attempt`, it is already lagging (restarting).
	
	pendingAttemptStr := pod.Annotations[leaderworkerset.PendingRestartAttemptAnnotationKey]
	pendingAttempt := 0
	if pendingAttemptStr != "" {
		var err error
		pendingAttempt, err = strconv.Atoi(pendingAttemptStr)
		if err != nil {
			return false, err
		}
	}

	if pendingAttempt < currentAttempt {
		// Already processing a restart for this group (or at least this pod is behind).
		return false, nil
	}

	// bump the restart attempt
	newAttempt := currentAttempt + 1
	newAttemptStr := strconv.Itoa(newAttempt)

	// Patch ALL pods in the group.
	// We need to list them all.
	pods, err := r.getPodsInGroup(ctx, lws, leader.Labels[leaderworkerset.GroupIndexLabelKey])
	if err != nil {
		return false, err
	}

	// We patch both RestartAttempt (Current) and Previous? 
	// JobSet uses separate "Current" and "Previous" in STATUS.
	// But maps them to annotations?
	// JobSet Controller:
	//   sets `jobset.sigs.k8s.io/restart-attempt` label on Job.
	//   The Agent reads?
	//   Wait, the Agent reads `CurrentInPlaceRestartAttempt` and `Previous...` from **JobSet Status**?
	//   "Get associated JobSet". Yes.
	//   The Agent watches the JOBSET.
	//   BUT I am writing an Agent for LWS.
	//   The Pod should validly be the source of truth if we use annotations.
	//   So I will use annotations on the Pod.
	//   To keep it atomic, I should patch all pods.
	
	patchFn := func(p corev1.Pod) error {
		patch := client.MergeFrom(p.DeepCopy())
		if p.Annotations == nil {
			p.Annotations = make(map[string]string)
		}
		p.Annotations[leaderworkerset.RestartAttemptAnnotationKey] = newAttemptStr
		// We also need "Previous"?
		// If we follow my "Latch" logic:
		// Previous is useful if we want to force a restart even if Agent is confused.
		// But `current > pending` is enough signal to "Restart until pending==current".
		// Agent Logic:
		//   pending = read(annotation)
		//   server = read(annotation_current)
		//   if pending < server:
		//      pending = server
		//      write(pending)
		//      exit()
		// This is sufficient! 
		// If pending < server, it means "I am outdated".
		// So I don't need "Previous".
		
		return r.Patch(ctx, &p, patch)
	}

	for _, p := range pods {
		if err := patchFn(p); err != nil {
			log.Error(err, "failed to patch pod with new restart attempt", "pod", p.Name)
			return false, err
		}
	}
	
	r.Record.Eventf(&lws, corev1.EventTypeNormal, "GroupsRestarting", "In-place restart triggered for group %s due to pod %s failure (attempt %d)", leader.Labels[leaderworkerset.GroupIndexLabelKey], pod.Name, newAttempt)

	return false, nil
}

func podInFailedState(pod corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodFailed {
		return true
	}
	// Check for CrashLoopBackOff or Error in waiting state
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, status := range statuses {
			if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
				return true
			}
			if status.State.Waiting != nil && (status.State.Waiting.Reason == "CrashLoopBackOff" || status.State.Waiting.Reason == "Error") {
				// We might want to be careful with CrashLoopBackOff if it's just starting up?
				// But generally yes, it's a failure.
				return true
			}
		}
	}
	return false
}

func (r *PodReconciler) getPodsInGroup(ctx context.Context, lws leaderworkerset.LeaderWorkerSet, groupIndex string) ([]corev1.Pod, error) {
	podSelector := client.MatchingLabels(map[string]string{
		leaderworkerset.SetNameLabelKey:    lws.Name,
		leaderworkerset.GroupIndexLabelKey: groupIndex,
	})
	var podList corev1.PodList
	if err := r.List(ctx, &podList, podSelector, client.InNamespace(lws.Namespace)); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func (r *PodReconciler) setNodeSelectorForWorkerPods(ctx context.Context, pod *corev1.Pod, sts *appsapplyv1.StatefulSetApplyConfiguration, topologyKey string) error {

	log := ctrl.LoggerFrom(ctx)
	topologyValue, err := r.topologyValueFromPod(ctx, pod, topologyKey)
	if err != nil {
		log.Error(err, "getting topology from leader pod")
		return err
	}

	// set node selector for worker pods, if worker pods already scheduled to different topology value
	// the following applying logic will automatically update it to match the leader pods, so we don't
	// need to verify if they have the same topology value
	sts.Spec.Template.Spec.WithNodeSelector(map[string]string{
		topologyKey: topologyValue,
	})
	return nil
}

func (r *PodReconciler) topologyValueFromPod(ctx context.Context, pod *corev1.Pod, topologyKey string) (string, error) {
	log := ctrl.LoggerFrom(ctx)

	nodeName := pod.Spec.NodeName
	ns := pod.Namespace

	// Get node the leader pod is running on.
	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, &node); err != nil {
		// We'll ignore not-found errors, since there is nothing we can do here.
		// A node may not exist temporarily due to a maintenance event or other scenarios.
		log.Error(err, fmt.Sprintf("getting node %s", nodeName))
		return "", client.IgnoreNotFound(err)
	}

	// Get topology (e.g. node pool name) from node labels.
	topology, exists := node.Labels[topologyKey]
	if !exists {
		return "", fmt.Errorf("node does not have topology label: %s", topology)
	}
	return topology, nil
}

func (r *PodReconciler) pendingPodsInGroup(ctx context.Context, pod corev1.Pod, groupSize int) (bool, error) {
	groupIndex := pod.Labels[leaderworkerset.GroupIndexLabelKey]
	lwsName := pod.Labels[leaderworkerset.SetNameLabelKey]

	podSelector := client.MatchingLabels(map[string]string{
		leaderworkerset.SetNameLabelKey:    lwsName,
		leaderworkerset.GroupIndexLabelKey: groupIndex,
	})

	var podList corev1.PodList
	if err := r.List(ctx, &podList, podSelector, client.InNamespace(pod.Namespace)); err != nil {
		return false, err
	}

	if groupSize != len(podList.Items) {
		return true, nil
	}

	for _, groupPod := range podList.Items {
		if groupPod.Status.Phase == corev1.PodPending {
			return true, nil
		}
	}
	return false, nil
}

// setControllerReferenceWithStatefulSet set controller reference for the StatefulSet
func setControllerReferenceWithStatefulSet(owner metav1.Object, sts *appsapplyv1.StatefulSetApplyConfiguration, scheme *runtime.Scheme) error {
	// Validate the owner.
	ro, ok := owner.(runtime.Object)
	if !ok {
		return fmt.Errorf("%T is not a runtime.Object, cannot call SetOwnerReference", owner)
	}
	gvk, err := apiutil.GVKForObject(ro, scheme)
	if err != nil {
		return err
	}
	sts.WithOwnerReferences(metaapplyv1.OwnerReference().
		WithAPIVersion(gvk.GroupVersion().String()).
		WithKind(gvk.Kind).
		WithName(owner.GetName()).
		WithUID(owner.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true))
	return nil
}

// constructWorkerStatefulSetApplyConfiguration constructs the applied configuration for the leader StatefulSet
func constructWorkerStatefulSetApplyConfiguration(leaderPod corev1.Pod, lws leaderworkerset.LeaderWorkerSet, currentRevision *appsv1.ControllerRevision) (*appsapplyv1.StatefulSetApplyConfiguration, error) {
	currentLws, err := revisionutils.ApplyRevision(&lws, currentRevision)
	if err != nil {
		return nil, err
	}
	podTemplateSpec := *currentLws.Spec.LeaderWorkerTemplate.WorkerTemplate.DeepCopy()
	if lws.Spec.LeaderWorkerTemplate.RestartPolicy == leaderworkerset.RecreateGroupInPlace {
		podutils.InjectInPlaceRestartSidecar(&podTemplateSpec.Spec)
	}
	// construct pod template spec configuration
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&podTemplateSpec)
	if err != nil {
		return nil, err
	}
	var podTemplateApplyConfiguration coreapplyv1.PodTemplateSpecApplyConfiguration
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(obj, &podTemplateApplyConfiguration)
	if err != nil {
		return nil, err
	}
	selectorMap := map[string]string{
		leaderworkerset.GroupIndexLabelKey:      leaderPod.Labels[leaderworkerset.GroupIndexLabelKey],
		leaderworkerset.SetNameLabelKey:         lws.Name,
		leaderworkerset.GroupUniqueHashLabelKey: leaderPod.Labels[leaderworkerset.GroupUniqueHashLabelKey],
	}
	labelMap := map[string]string{
		leaderworkerset.GroupIndexLabelKey:      leaderPod.Labels[leaderworkerset.GroupIndexLabelKey],
		leaderworkerset.SetNameLabelKey:         lws.Name,
		leaderworkerset.GroupUniqueHashLabelKey: leaderPod.Labels[leaderworkerset.GroupUniqueHashLabelKey],
		leaderworkerset.RevisionKey:             revisionutils.GetRevisionKey(&leaderPod),
	}

	podTemplateApplyConfiguration.WithLabels(labelMap)
	podAnnotations := make(map[string]string)
	podAnnotations[leaderworkerset.SizeAnnotationKey] = strconv.Itoa(int(*lws.Spec.LeaderWorkerTemplate.Size))
	podAnnotations[leaderworkerset.LeaderPodNameAnnotationKey] = leaderPod.Name
	if lws.Annotations[leaderworkerset.ExclusiveKeyAnnotationKey] != "" {
		podAnnotations[leaderworkerset.ExclusiveKeyAnnotationKey] = lws.Annotations[leaderworkerset.ExclusiveKeyAnnotationKey]
	}
	if lws.Spec.LeaderWorkerTemplate.SubGroupPolicy != nil {
		podAnnotations[leaderworkerset.SubGroupSizeAnnotationKey] = strconv.Itoa(int(*lws.Spec.LeaderWorkerTemplate.SubGroupPolicy.SubGroupSize))
		if lws.Annotations[leaderworkerset.SubGroupExclusiveKeyAnnotationKey] != "" {
			podAnnotations[leaderworkerset.SubGroupExclusiveKeyAnnotationKey] = lws.Annotations[leaderworkerset.SubGroupExclusiveKeyAnnotationKey]
		}
	}
	acceleratorutils.AddTPUAnnotations(leaderPod, podAnnotations)
	podTemplateApplyConfiguration.WithAnnotations(podAnnotations)
	serviceName := leaderPod.Name
	if lws.Spec.NetworkConfig == nil || *lws.Spec.NetworkConfig.SubdomainPolicy == leaderworkerset.SubdomainShared {
		serviceName = lws.Name
	}
	// construct statefulset apply configuration
	statefulSetConfig := appsapplyv1.StatefulSet(leaderPod.Name, leaderPod.Namespace).
		WithSpec(appsapplyv1.StatefulSetSpec().
			WithServiceName(serviceName).
			WithReplicas(*lws.Spec.LeaderWorkerTemplate.Size - 1).
			WithPodManagementPolicy(appsv1.ParallelPodManagement).
			WithTemplate(&podTemplateApplyConfiguration).
			WithOrdinals(appsapplyv1.StatefulSetOrdinals().WithStart(1)).
			WithSelector(metaapplyv1.LabelSelector().
				WithMatchLabels(selectorMap))).
		WithLabels(labelMap)

	pvcApplyConfiguration := controllerutils.GetPVCApplyConfiguration(&lws)
	if len(pvcApplyConfiguration) > 0 {
		statefulSetConfig.Spec.WithVolumeClaimTemplates(pvcApplyConfiguration...)
	}

	if lws.Spec.LeaderWorkerTemplate.PersistentVolumeClaimRetentionPolicy != nil {
		pvcRetentionPolicy := &appsapplyv1.StatefulSetPersistentVolumeClaimRetentionPolicyApplyConfiguration{
			WhenDeleted: &lws.Spec.LeaderWorkerTemplate.PersistentVolumeClaimRetentionPolicy.WhenDeleted,
			WhenScaled:  &lws.Spec.LeaderWorkerTemplate.PersistentVolumeClaimRetentionPolicy.WhenScaled,
		}
		statefulSetConfig.Spec.WithPersistentVolumeClaimRetentionPolicy(pvcRetentionPolicy)
	}

	// Inject in-place restart sidecar if enabled
	if lws.Spec.LeaderWorkerTemplate.RestartPolicy == leaderworkerset.RecreateGroupInPlace {
		// Use a helper that works on *PodSpecApplyConfiguration if possible, or just modify the ApplyConfiguration struct directly?
		// modify the `podTemplateApplyConfiguration` before it is added to `statefulSetConfig`?
		// But `podTemplateApplyConfiguration` was converted from unstructured.
		// It's convoluted.
		// `podTemplateSpec` is a corev1.PodTemplateSpec. We can modify that BEFORE converting to unstructured!
		// Line 349: podTemplateSpec := *currentLws.Spec.LeaderWorkerTemplate.WorkerTemplate.DeepCopy()
		
		// Actually, let's look at where `podTemplateSpec` is used.
		// It is used to create `obj`.
		// We should inject BEFORE that.
		// BUT `constructWorkerStatefulSetApplyConfiguration` is the function we are in.
		// We need to modify `podTemplateSpec` before line 351.
		
		// Wait, I can't easily modify the code block at line 351 because I am replacing closing brace?
		// No, I am replacing the end of the function.
		// I should use `multi_replace` to target the START of the function or just edit the `podTemplateSpec` near the top.
		
		// Refactor: I'll just do it on the `statefulSetConfig`?
		// `statefulSetConfig.Spec.Template.Spec.Containers`?
		// It is an ApplyConfiguration.
		// `WithContainers` appends? No, it replaces list.
		// Accessing the list is hard.
		
		// Better: Update `currentLws` at the start?
		// `currentLws` is derived from `revisonutils.ApplyRevision`.
		
		// Let's modify the `podTemplateSpec` logic.
		// I will do another replacement for the start of the function.
	}
	return statefulSetConfig, nil
}

func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(object client.Object) bool {
			if pod, ok := object.(*corev1.Pod); ok {
				_, exist := pod.Labels[leaderworkerset.SetNameLabelKey]
				return exist
			}
			if statefulSet, ok := object.(*appsv1.StatefulSet); ok {
				_, exist := statefulSet.Labels[leaderworkerset.SetNameLabelKey]
				return exist
			}
			return false
		})).Owns(&appsv1.StatefulSet{}).Complete(r)
}

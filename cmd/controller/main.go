/*
Copyright 2026.

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
	"flag"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	lwsclientset "sigs.k8s.io/lws/client-go/clientset/versioned"
)

const (
	rebootLabel = "cloud.google.com/perform-reboot"
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	klog.Info("Starting controller sidecar...")

	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Error building in-cluster config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %v", err)
	}

	lwsClient, err := lwsclientset.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Error building lws clientset: %v", err)
	}

	ctx := context.Background()

	for {
		// Fetch pods
		pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
		if err != nil {
			klog.Errorf("Error listing pods: %v", err)
		} else {
			klog.Infof("Successfully fetched %d pods", len(pods.Items))
		}

		// Fetch LWS objects
		lwsList, err := lwsClient.LeaderworkersetV1().LeaderWorkerSets("").List(ctx, metav1.ListOptions{})
		if err != nil {
			klog.Errorf("Error listing leaderworkersets: %v", err)
		} else {
			klog.Infof("Successfully fetched %d leaderworkersets", len(lwsList.Items))
		}

		// Fetch nodes
		nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			klog.Errorf("Error listing nodes: %v", err)
		} else {
			klog.Infof("Successfully fetched %d nodes", len(nodes.Items))
		}

		time.Sleep(10 * time.Second)
	}
}

// patchNodeLabel patches the "cloud.google.com/perform-reboot=true" label onto the specified node.
func patchNodeLabel(ctx context.Context, clientset *kubernetes.Clientset, nodeName string) error {
	patchData := fmt.Sprintf(`{"metadata":{"labels":{"%s":"true"}}}`, rebootLabel)
	_, err := clientset.CoreV1().Nodes().Patch(ctx, nodeName, types.StrategicMergePatchType, []byte(patchData), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch node %s: %v", nodeName, err)
	}
	klog.Infof("Successfully patched node %s with reboot label", nodeName)
	return nil
}

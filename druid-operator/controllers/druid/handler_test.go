/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package druid

import (
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/ghodss/yaml"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"

	druidv1alpha1 "github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
)

// +kubebuilder:docs-gen:collapse=Imports

// testHandler
var _ = Describe("Test handler", func() {
	Context("When testing handler", func() {
		It("should make statefulset for broker", func() {
			By("By making statefulset for broker")
			filePath := "testdata/druid-test-cr.yaml"
			clusterSpec, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())

			nodeSpecUniqueStr := makeNodeSpecificUniqueString(clusterSpec, "brokers")
			nodeSpec := clusterSpec.Spec.Nodes["brokers"]

			actual, _ := makeStatefulSet(&nodeSpec, clusterSpec, makeLabelsForNodeSpec(&nodeSpec, clusterSpec, clusterSpec.Name, nodeSpecUniqueStr), nodeSpecUniqueStr, "blah", nodeSpecUniqueStr)
			addHashToObject(actual)

			expected := new(appsv1.StatefulSet)
			err = readAndUnmarshallResource("testdata/broker-statefulset.yaml", &expected)
			Expect(err).Should(BeNil())

			Expect(actual).Should(Equal(expected))
		})
		It("should make statefulset for broker without default probe", func() {
			By("By making statefulset for broker")
			filePath := "testdata/druid-test-cr-noprobe.yaml"
			clusterSpec, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())

			nodeSpecUniqueStr := makeNodeSpecificUniqueString(clusterSpec, "brokers")
			nodeSpec := clusterSpec.Spec.Nodes["brokers"]

			actual, _ := makeStatefulSet(&nodeSpec, clusterSpec, makeLabelsForNodeSpec(&nodeSpec, clusterSpec, clusterSpec.Name, nodeSpecUniqueStr), nodeSpecUniqueStr, "blah", nodeSpecUniqueStr)
			addHashToObject(actual)

			expected := new(appsv1.StatefulSet)
			err = readAndUnmarshallResource("testdata/broker-statefulset-noprobe.yaml", &expected)
			Expect(err).Should(BeNil())

			Expect(actual).Should(Equal(expected))
		})

		It("should make statefulset for broker with sidecar", func() {
			By("By making statefulset for broker with sidecar")
			filePath := "testdata/druid-test-cr-sidecar.yaml"
			clusterSpec, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())

			nodeSpecUniqueStr := makeNodeSpecificUniqueString(clusterSpec, "brokers")
			nodeSpec := clusterSpec.Spec.Nodes["brokers"]

			actual, _ := makeStatefulSet(&nodeSpec, clusterSpec, makeLabelsForNodeSpec(&nodeSpec, clusterSpec, clusterSpec.Name, nodeSpecUniqueStr), nodeSpecUniqueStr, "blah", nodeSpecUniqueStr)
			addHashToObject(actual)

			expected := new(appsv1.StatefulSet)
			readAndUnmarshallResource("testdata/broker-statefulset-sidecar.yaml", &expected)
			Expect(err).Should(BeNil())

			Expect(actual).Should(Equal(expected))
		})

		It("should make deployment for broker", func() {
			By("By making deployment for broker")
			filePath := "testdata/druid-test-cr.yaml"
			clusterSpec, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())

			nodeSpecUniqueStr := makeNodeSpecificUniqueString(clusterSpec, "brokers")
			nodeSpec := clusterSpec.Spec.Nodes["brokers"]

			actual, _ := makeDeployment(&nodeSpec, clusterSpec, makeLabelsForNodeSpec(&nodeSpec, clusterSpec, clusterSpec.Name, nodeSpecUniqueStr), nodeSpecUniqueStr, "blah", nodeSpecUniqueStr)
			addHashToObject(actual)

			expected := new(appsv1.Deployment)
			readAndUnmarshallResource("testdata/broker-deployment.yaml", &expected)
			Expect(err).Should(BeNil())

			Expect(actual).Should(Equal(expected))
		})

		It("should make PDB for broker", func() {
			By("By making PDB for broker")
			filePath := "testdata/druid-test-cr.yaml"
			clusterSpec, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())

			nodeSpecUniqueStr := makeNodeSpecificUniqueString(clusterSpec, "brokers")
			nodeSpec := clusterSpec.Spec.Nodes["brokers"]

			actual, _ := makePodDisruptionBudget(&nodeSpec, clusterSpec, makeLabelsForNodeSpec(&nodeSpec, clusterSpec, clusterSpec.Name, nodeSpecUniqueStr), nodeSpecUniqueStr)
			addHashToObject(actual)

			expected := new(policyv1.PodDisruptionBudget)
			readAndUnmarshallResource("testdata/broker-pod-disruption-budget.yaml", &expected)
			Expect(err).Should(BeNil())

			Expect(actual).Should(Equal(expected))
		})

		It("should make headless service", func() {
			By("By making headless service")
			filePath := "testdata/druid-test-cr.yaml"
			clusterSpec, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())

			nodeSpecUniqueStr := makeNodeSpecificUniqueString(clusterSpec, "brokers")
			nodeSpec := clusterSpec.Spec.Nodes["brokers"]

			actual, _ := makeService(&nodeSpec.Services[0], &nodeSpec, clusterSpec, makeLabelsForNodeSpec(&nodeSpec, clusterSpec, clusterSpec.Name, nodeSpecUniqueStr), nodeSpecUniqueStr)
			addHashToObject(actual)

			expected := new(corev1.Service)
			readAndUnmarshallResource("testdata/broker-headless-service.yaml", &expected)
			Expect(err).Should(BeNil())

			Expect(actual).Should(Equal(expected))
		})

		It("should make load balancer service", func() {
			By("By making load balancer service")
			filePath := "testdata/druid-test-cr.yaml"
			clusterSpec, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())

			nodeSpecUniqueStr := makeNodeSpecificUniqueString(clusterSpec, "brokers")
			nodeSpec := clusterSpec.Spec.Nodes["brokers"]

			actual, _ := makeService(&nodeSpec.Services[1], &nodeSpec, clusterSpec, makeLabelsForNodeSpec(&nodeSpec, clusterSpec, clusterSpec.Name, nodeSpecUniqueStr), nodeSpecUniqueStr)
			addHashToObject(actual)

			expected := new(corev1.Service)
			readAndUnmarshallResource("testdata/broker-load-balancer-service.yaml", &expected)
			Expect(err).Should(BeNil())

			Expect(actual).Should(Equal(expected))
		})

		It("should make config map", func() {
			By("By making config map")
			filePath := "testdata/druid-test-cr.yaml"
			clusterSpec, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())
			actual, _ := makeCommonConfigMap(ctx, k8sClient, clusterSpec, makeLabelsForDruid(clusterSpec))
			addHashToObject(actual)

			expected := new(corev1.ConfigMap)
			readAndUnmarshallResource("testdata/common-config-map.yaml", &expected)
			Expect(err).Should(BeNil())

			Expect(actual).Should(Equal(expected))
		})

		It("should make broker config map", func() {
			By("By making broker config map")
			filePath := "testdata/druid-test-cr.yaml"
			clusterSpec, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())

			nodeSpecUniqueStr := makeNodeSpecificUniqueString(clusterSpec, "brokers")
			nodeSpec := clusterSpec.Spec.Nodes["brokers"]

			actual, _ := makeConfigMapForNodeSpec(&nodeSpec, clusterSpec, makeLabelsForNodeSpec(&nodeSpec, clusterSpec, clusterSpec.Name, nodeSpecUniqueStr), nodeSpecUniqueStr)
			addHashToObject(actual)

			expected := new(corev1.ConfigMap)
			readAndUnmarshallResource("testdata/broker-config-map.yaml", &expected)
			Expect(err).Should(BeNil())

			Expect(actual).Should(Equal(expected))
		})

	})
})

func readDruidClusterSpecFromFile(filePath string) (*druidv1alpha1.Druid, error) {
	clusterSpec := new(druidv1alpha1.Druid)
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return clusterSpec, err
	}

	err = yaml.Unmarshal(bytes, &clusterSpec)
	if err != nil {
		return clusterSpec, err
	}
	return clusterSpec, nil
}

func readAndUnmarshallResource(file string, res interface{}) error {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(bytes, res)
	if err != nil {
		return err
	}
	return nil
}

func TestPodSpecDNSConfig(t *testing.T) {
	tests := []struct {
		name          string
		nodeDNSConfig *corev1.PodDNSConfig
		specDNSConfig *corev1.PodDNSConfig
		expected      *corev1.PodDNSConfig
	}{
		{
			name:          "Both nil",
			nodeDNSConfig: nil,
			specDNSConfig: nil,
			expected:      nil,
		},
		{
			name:          "Only spec provided",
			nodeDNSConfig: nil,
			specDNSConfig: &corev1.PodDNSConfig{
				Nameservers: []string{"8.8.8.8"},
				Searches:    []string{"example.com"},
			},
			expected: &corev1.PodDNSConfig{
				Nameservers: []string{"8.8.8.8"},
				Searches:    []string{"example.com"},
			},
		},
		{
			name: "Only node provided",
			nodeDNSConfig: &corev1.PodDNSConfig{
				Nameservers: []string{"1.1.1.1"},
				Searches:    []string{"node.local"},
			},
			specDNSConfig: nil,
			expected: &corev1.PodDNSConfig{
				Nameservers: []string{"1.1.1.1"},
				Searches:    []string{"node.local"},
			},
		},
		{
			name: "Both provided, node wins",
			nodeDNSConfig: &corev1.PodDNSConfig{
				Nameservers: []string{"1.1.1.1"},
				Searches:    []string{"node.local"},
			},
			specDNSConfig: &corev1.PodDNSConfig{
				Nameservers: []string{"8.8.8.8"},
				Searches:    []string{"example.com"},
			},
			expected: &corev1.PodDNSConfig{
				Nameservers: []string{"1.1.1.1"},
				Searches:    []string{"node.local"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &druidv1alpha1.Druid{
				Spec: druidv1alpha1.DruidSpec{
					DNSConfig: tc.specDNSConfig,
				},
			}
			nodeSpec := &druidv1alpha1.DruidNodeSpec{
				DNSConfig: tc.nodeDNSConfig,
			}
			podSpec := makePodSpec(nodeSpec, m, "unique", "dummySHA")
			if !reflect.DeepEqual(podSpec.DNSConfig, tc.expected) {
				t.Errorf("expected DNSConfig %v, got %v", tc.expected, podSpec.DNSConfig)
			}
		})
	}
}

func TestPodSpecDNSConfigYAML(t *testing.T) {
	m, err := readDruidClusterSpecFromFile("testdata/druid-test-cr.yaml")
	if err != nil {
		t.Fatalf("failed to read cluster spec: %v", err)
	}
	nodeSpec := m.Spec.Nodes["middlemanagers"]
	podSpec := makePodSpec(&nodeSpec, m, "unique", "dummySHA")
	expectedDNSConfig := &corev1.PodDNSConfig{
		Nameservers: []string{"10.0.0.53"},
		Searches:    []string{"example.local"},
	}
	if !reflect.DeepEqual(podSpec.DNSConfig, expectedDNSConfig) {
		t.Errorf("expected DNSConfig %v, got %v", expectedDNSConfig, podSpec.DNSConfig)
	}
}

// TestPodSpecDNSPolicy verifies DNSPolicy resolution in makePodSpec.
func TestPodSpecDNSPolicy(t *testing.T) {
	tests := []struct {
		name     string
		nodeDNS  string
		specDNS  string
		expected corev1.DNSPolicy
	}{
		{"Both empty", "", "", corev1.DNSPolicy("")},
		{"Only spec provided", "", "ClusterFirst", corev1.DNSPolicy("ClusterFirst")},
		{"Only node provided", "Default", "", corev1.DNSPolicy("Default")},
		{"Both provided, node wins", "Default", "ClusterFirst", corev1.DNSPolicy("Default")},
	}

	for _, tc := range tests {
		tc := tc // capture current test case
		t.Run(tc.name, func(t *testing.T) {
			m := &druidv1alpha1.Druid{
				Spec: druidv1alpha1.DruidSpec{
					DNSPolicy: corev1.DNSPolicy(tc.specDNS),
				},
			}
			nodeSpec := &druidv1alpha1.DruidNodeSpec{
				DNSPolicy: corev1.DNSPolicy(tc.nodeDNS),
			}
			podSpec := makePodSpec(nodeSpec, m, "unique", "dummySHA")
			if podSpec.DNSPolicy != tc.expected {
				t.Errorf("expected DNSPolicy %q, got %q", tc.expected, podSpec.DNSPolicy)
			}
		})
	}
}

// TestPodSpecDNSPolicyYAML validates that the generated PodSpec DNSPolicy matches the expected value,
// using the druid-test-cr.yaml as the single input file.
func TestPodSpecDNSPolicyYAML(t *testing.T) {
	m, err := readDruidClusterSpecFromFile("testdata/druid-test-cr.yaml")
	if err != nil {
		t.Fatalf("failed to read cluster spec: %v", err)
	}
	nodeSpec := m.Spec.Nodes["middlemanagers"]
	podSpec := makePodSpec(&nodeSpec, m, "unique", "dummySHA")
	expectedDNSPolicy := corev1.DNSPolicy("ClusterFirst")
	if podSpec.DNSPolicy != expectedDNSPolicy {
		t.Errorf("expected DNSPolicy %q, got %q", expectedDNSPolicy, podSpec.DNSPolicy)
	}
}

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
	"time"

	druidv1alpha1 "github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
)

// +kubebuilder:docs-gen:collapse=Imports

/*
volume_expansion_test
*/
var _ = Describe("Test volume expansion feature", func() {
	const (
		filePath = "testdata/volume-expansion.yaml"
		timeout  = time.Second * 45
		interval = time.Millisecond * 250
	)

	var (
		druid = &druidv1alpha1.Druid{}
	)

	Context("When creating a druid cluster with volume expansion", func() {
		It("Should create the druid object", func() {
			By("Creating a new druid")
			druidCR, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())
			Expect(k8sClient.Create(ctx, druidCR)).To(Succeed())

			By("Getting a newly created druid")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druidCR.Name, Namespace: druidCR.Namespace}, druid)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})
		It("Should error on the CR verify stage if storage class is nil", func() {
			By("Setting storage class name to nil")
			druid.Spec.Nodes["historicals"].VolumeClaimTemplates[0].Spec.StorageClassName = nil
			Expect(druid.Spec.Nodes["historicals"].VolumeClaimTemplates[0].Spec.StorageClassName).Should(BeNil())

			By("Validating the created druid")
			Expect(validateVolumeClaimTemplateSpec(druid)).Error()
		})
		It("Should error if validate didn't worked and storageClassName does not exists", func() {
			By("By getting the historicals nodeSpec")
			allNodeSpecs := getNodeSpecsByOrder(druid)

			nodeSpec := &druidv1alpha1.DruidNodeSpec{}
			for _, elem := range allNodeSpecs {
				if elem.key == "historicals" {
					nodeSpec = &elem.spec
				}
			}
			Expect(nodeSpec).ShouldNot(BeNil())

			By("By calling the expand volume function with storageClass nil")
			Expect(isVolumeExpansionEnabled(ctx, k8sClient, druid, nodeSpec, nil)).Error()
		})
	})
})

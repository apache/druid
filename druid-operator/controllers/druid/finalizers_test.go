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
finalizers_test
*/
var _ = Describe("Test finalizers logic", func() {
	const (
		filePath = "testdata/finalizers.yaml"
		timeout  = time.Second * 45
		interval = time.Millisecond * 250
	)

	var (
		druid = &druidv1alpha1.Druid{}
	)

	Context("When creating a druid cluster", func() {
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
		It("Should add the delete PVC finalizer", func() {
			By("Waiting for the finalizer to be created")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druid.Name, Namespace: druid.Namespace}, druid)
				if err == nil && ContainsString(druid.GetFinalizers(), deletePVCFinalizerName) {
					return true
				}
				return false
			}, timeout, interval).Should(BeTrue())
		})
		It("Should delete druid successfully", func() {
			By("Waiting for the druid cluster to be deleted")
			Expect(k8sClient.Delete(ctx, druid)).To(Succeed())
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druid.Name, Namespace: druid.Namespace}, druid)
				return err != nil
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("When creating a druid cluster with disablePVCDeletion", func() {
		It("Should create the druid object", func() {
			By("Creating a new druid")
			druidCR, err := readDruidClusterSpecFromFile(filePath)
			druidCR.Spec.DisablePVCDeletionFinalizer = true
			Expect(err).Should(BeNil())
			Expect(k8sClient.Create(ctx, druidCR)).To(Succeed())

			By("Getting a newly created druid")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druidCR.Name, Namespace: druidCR.Namespace}, druid)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})
		It("Should not add the delete PVC finalizer", func() {
			By("Call for the update finalizer function")
			Expect(updateFinalizers(ctx, k8sClient, druid, emitEvent)).Should(BeNil())

			By("Getting a updated druid")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druid.Name, Namespace: druid.Namespace}, druid)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			By("Checking the absence of the finalizer")
			Expect(ContainsString(druid.GetFinalizers(), deletePVCFinalizerName)).Should(BeFalse())
		})
		It("Should delete druid successfully", func() {
			Expect(k8sClient.Delete(ctx, druid)).To(Succeed())
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druid.Name, Namespace: druid.Namespace}, druid)
				return err != nil
			}, timeout, interval).Should(BeTrue())
		})
	})

	Context("When creating a druid cluster", func() {
		It("Should create the druid object", func() {
			By("Creating a new druid")
			druidCR, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())
			Expect(k8sClient.Create(ctx, druidCR)).To(Succeed())

			By("Getting the CR")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druidCR.Name, Namespace: druidCR.Namespace}, druid)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})
		It("Should add the delete PVC finalizer", func() {
			By("Waiting for the finalizer to be created")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druid.Name, Namespace: druid.Namespace}, druid)
				if err == nil && ContainsString(druid.GetFinalizers(), deletePVCFinalizerName) {
					return true
				}
				return false
			}, timeout, interval).Should(BeTrue())
		})
		It("Should remove the delete PVC finalizer", func() {
			By("Disabling the deletePVC finalizer")
			druid.Spec.DisablePVCDeletionFinalizer = true
			Expect(k8sClient.Update(ctx, druid)).To(BeNil())
			By("Waiting for the finalizer to be deleted")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druid.Name, Namespace: druid.Namespace}, druid)
				if err == nil && !ContainsString(druid.GetFinalizers(), deletePVCFinalizerName) {
					return true
				}
				return false
			}, timeout, interval).Should(BeTrue())
		})
		It("Should delete druid successfully", func() {
			Expect(k8sClient.Delete(ctx, druid)).To(Succeed())
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druid.Name, Namespace: druid.Namespace}, druid)
				return err != nil
			}, timeout, interval).Should(BeTrue())
		})
	})
})

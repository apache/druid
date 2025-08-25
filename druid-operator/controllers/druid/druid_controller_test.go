package druid

import (
	"fmt"
	"time"

	druidv1alpha1 "github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalev2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
)

// +kubebuilder:docs-gen:collapse=Imports

/*
testDruidOperator
*/
var _ = Describe("Druid Operator", func() {

	// Define utility constants for object names and testing timeouts/durations and intervals.
	const (
		filePath = "testdata/druid-smoke-test-cluster.yaml"
		timeout  = time.Second * 45
		interval = time.Millisecond * 250
	)
	druid := &druidv1alpha1.Druid{}
	druidComponents := []string{"coordinators", "historicals", "routers", "brokers"}

	Context("When testing Druid Operator", func() {
		druidCR, err := readDruidClusterSpecFromFile(filePath)
		Expect(err).Should(BeNil())

		It("Test druidCR creation - testDruidOperator", func() {
			By("By creating a new druidCR")
			Expect(k8sClient.Create(ctx, druidCR)).To(Succeed())

			// Get CR and match ConfigMaps
			expectedConfigMaps := []string{
				fmt.Sprintf("druid-%s-brokers-config", druidCR.Name),
				fmt.Sprintf("druid-%s-coordinators-config", druidCR.Name),
				fmt.Sprintf("druid-%s-historicals-config", druidCR.Name),
				fmt.Sprintf("druid-%s-routers-config", druidCR.Name),
				fmt.Sprintf("%s-druid-common-config", druidCR.Name),
			}

			By("By getting a newly created druidCR")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druidCR.Name, Namespace: druidCR.Namespace}, druid)
				if !areStringArraysEqual(druid.Status.ConfigMaps, expectedConfigMaps) {
					return false
				}
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Match ConfigMaps
			By("By matching ConfigMaps")
			Expect(druid.Status.ConfigMaps).Should(ConsistOf(expectedConfigMaps))

			// Match Services
			By("By matching Services")
			expectedServices := []string{
				fmt.Sprintf("druid-%s-brokers", druidCR.Name),
				fmt.Sprintf("druid-%s-coordinators", druidCR.Name),
				fmt.Sprintf("druid-%s-historicals", druidCR.Name),
				fmt.Sprintf("druid-%s-routers", druidCR.Name),
			}
			Expect(druid.Status.Services).Should(ConsistOf(expectedServices))

			// Match StatefulSets
			By("By matching StatefulSets")
			expectedStatefulSets := []string{
				fmt.Sprintf("druid-%s-coordinators", druidCR.Name),
				fmt.Sprintf("druid-%s-historicals", druidCR.Name),
				fmt.Sprintf("druid-%s-routers", druidCR.Name),
			}
			Expect(druid.Status.StatefulSets).Should(ConsistOf(expectedStatefulSets))

			// Match Deployments
			By("By matching Deployments")
			expectedDeployments := []string{
				fmt.Sprintf("druid-%s-brokers", druidCR.Name),
			}
			Expect(druid.Status.Deployments).Should(ConsistOf(expectedDeployments))

			// Match PDBs
			By("By matching PDBs")
			expectedPDBs := []string{
				fmt.Sprintf("druid-%s-brokers", druidCR.Name),
			}
			Expect(druid.Status.PodDisruptionBudgets).Should(ConsistOf(expectedPDBs))

			// Match HPAs
			By("By matching HPAs")
			expectedHPAs := []string{
				fmt.Sprintf("druid-%s-brokers", druidCR.Name),
			}
			Expect(druid.Status.HPAutoScalers).Should(ConsistOf(expectedHPAs))

			// Match Ingress
			By("By matching Ingress")
			expectedIngress := []string{
				fmt.Sprintf("druid-%s-routers", druidCR.Name),
			}
			Expect(druid.Status.Ingress).Should(ConsistOf(expectedIngress))

		})

		It("Test broker deployment", func() {
			componentName := "brokers"
			createdDeploy := &appsv1.Deployment{}
			brokerDeployment := fmt.Sprintf("druid-%s-%s", druidCR.Name, componentName)
			depNamespacedName := types.NamespacedName{Name: brokerDeployment, Namespace: druidCR.Namespace}

			// Match Deployment replicas
			By("By getting deployment and checking replicas")

			Eventually(func() bool {
				err := k8sClient.Get(ctx, depNamespacedName, createdDeploy)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(*createdDeploy.Spec.Replicas).To(Equal(druidCR.Spec.Nodes[componentName].Replicas))

			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druidCR.Name, Namespace: druidCR.Namespace}, druid)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			By("By updating broker deployment replicas")
			replicaCount := 2
			if druidRep, ok := druid.Spec.Nodes[componentName]; ok {
				druidRep.Replicas = int32(replicaCount)
				druid.Spec.Nodes[componentName] = druidRep
			}
			// updating CR
			Expect(k8sClient.Update(ctx, druid)).Should(Succeed())

			// Fetch druid CR and check replicas
			Eventually(func() bool {
				k8sClient.Get(ctx, depNamespacedName, druid)
				return druid.Spec.Nodes[componentName].Replicas == 2
			}, timeout, interval).Should(BeTrue())

			// Fetch deployment and check replicas
			Eventually(func() bool {
				k8sClient.Get(ctx, depNamespacedName, createdDeploy)
				return *createdDeploy.Spec.Replicas == 2
			}, timeout, interval).Should(BeTrue())
		})

		// Test statefulsets replica count and update the replica count then match
		expectedStatefulSets := []string{"coordinators", "historicals", "routers"}
		for _, componentName := range expectedStatefulSets {
			componentName := componentName
			It(fmt.Sprintf("Test statefulset for %s", componentName), func() {
				createdSts := &appsv1.StatefulSet{}
				stsName := fmt.Sprintf("druid-%s-%s", druidCR.Name, componentName)
				stsNamespacedName := types.NamespacedName{Name: stsName, Namespace: druidCR.Namespace}

				// Match statefulset replicas
				By(fmt.Sprintf("By getting statefulset and checking replicas for %s ", stsName))
				Eventually(func() bool {
					err := k8sClient.Get(ctx, stsNamespacedName, createdSts)
					return err == nil
				}, timeout, interval).Should(BeTrue())
				Expect(*createdSts.Spec.Replicas).To(Equal(druidCR.Spec.Nodes[componentName].Replicas))

				By(fmt.Sprintf("By updating statefulset replicas %s ", stsName))
				replicaCount := 2
				if druidRep, ok := druid.Spec.Nodes[componentName]; ok {
					druidRep.Replicas = int32(replicaCount)
					druid.Spec.Nodes[componentName] = druidRep
				}
				// updating CR
				Expect(k8sClient.Update(ctx, druid)).Should(Succeed())

				// Fetch druid CR and check replicas
				Eventually(func() bool {
					k8sClient.Get(ctx, types.NamespacedName{Name: druidCR.Name, Namespace: druidCR.Namespace}, druid)
					return druid.Spec.Nodes[componentName].Replicas == 2
				}, timeout, interval).Should(BeTrue())

				// Fetch statefulset and check replicas
				Eventually(func() bool {
					k8sClient.Get(ctx, stsNamespacedName, createdSts)
					return *createdSts.Spec.Replicas == 2
				}, timeout, interval).Should(BeTrue())
			})
		}

		// Test statefulsets replica count and update the replica count then match
		for _, componentName := range druidComponents {
			componentName := componentName
			It(fmt.Sprintf("Test kubernetes service for %s", componentName), func() {
				createdService := &v1.Service{}
				serviceName := fmt.Sprintf("druid-%s-%s", druidCR.Name, componentName)
				serviceNamespacedName := types.NamespacedName{Name: serviceName, Namespace: druidCR.Namespace}

				// Checking  the kubernetes service Type
				By(fmt.Sprintf("By checking kubernetes service type %s ", serviceName))
				Eventually(func() bool {
					err := k8sClient.Get(ctx, serviceNamespacedName, createdService)
					return err == nil
				}, timeout, interval).Should(BeTrue())
				Expect(createdService.Spec.Type).To(Equal(druidCR.Spec.Services[0].Spec.Type))
				// Kubernetes service check for targetport and port
				By(fmt.Sprintf("By checking kubernetes service port for %s ", serviceName))
				Expect(createdService.Spec.Ports[0].Port).To(Equal(druidCR.Spec.Nodes[componentName].DruidPort))
				Expect(createdService.Spec.Ports[0].TargetPort.IntVal).To(Equal(druidCR.Spec.Nodes[componentName].DruidPort))
			})
		}
		// Check for pod distruption budget
		It("Test poddisruptionbudget for broker", func() {
			componentName := "brokers"
			createdPdb := &policyv1.PodDisruptionBudget{}
			pdbName := fmt.Sprintf("druid-%s-%s", druidCR.Name, componentName)
			pdbNamespacedName := types.NamespacedName{Name: pdbName, Namespace: druidCR.Namespace}

			// Checking  the kubernetes service Type
			By(fmt.Sprintf("By checking kubernetes poddisruptionbudget for  %s ", pdbName))

			Eventually(func() bool {
				err := k8sClient.Get(ctx, pdbNamespacedName, createdPdb)
				return err == nil
			}, timeout, interval).Should(BeTrue())
			Expect(createdPdb.Spec.MinAvailable).To(Equal(druidCR.Spec.Nodes[componentName].PodDisruptionBudgetSpec.MinAvailable))
		})
		// Check for ingress
		It("Test ingress for router", func() {
			componentName := "routers"
			createdIngress := &netv1.Ingress{}
			ingressName := fmt.Sprintf("druid-%s-%s", druidCR.Name, componentName)
			ingressNamespacedName := types.NamespacedName{Name: ingressName, Namespace: druidCR.Namespace}

			// Checking  the kubernetes service Type
			By(fmt.Sprintf("By checking ingress for %s ", ingressName))
			Eventually(func() bool {
				err := k8sClient.Get(ctx, ingressNamespacedName, createdIngress)
				return err == nil
			}, timeout, interval).Should(BeTrue())
			// check host and validate the domain
			Expect(createdIngress.Spec.Rules[0].Host).To(Equal(druidCR.Spec.Nodes[componentName].Ingress.Rules[0].Host))
			// check service name
			Expect(createdIngress.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Name).To(Equal(druidCR.Spec.Nodes[componentName].Ingress.Rules[0].HTTP.Paths[0].Backend.Service.Name))
			// check service port name
			Expect(createdIngress.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Name).To(Equal(druidCR.Spec.Nodes[componentName].Ingress.Rules[0].HTTP.Paths[0].Backend.Service.Port.Name))
			// check ingress path
			Expect(createdIngress.Spec.Rules[0].HTTP.Paths[0].Path).To(Equal(druidCR.Spec.Nodes[componentName].Ingress.Rules[0].HTTP.Paths[0].Path))
			// check ingress pathtype
			Expect(*createdIngress.Spec.Rules[0].HTTP.Paths[0].PathType).To(Equal(*druidCR.Spec.Nodes[componentName].Ingress.Rules[0].HTTP.Paths[0].PathType))
			// check tls hostname
			Expect(createdIngress.Spec.TLS[0].Hosts).To(Equal(druidCR.Spec.Nodes[componentName].Ingress.TLS[0].Hosts))
			// check tls secret name
			Expect(createdIngress.Spec.TLS[0].SecretName).To(Equal(druidCR.Spec.Nodes[componentName].Ingress.TLS[0].SecretName))
		})
		// Check for pod distruption budget
		It("Test common configmap", func() {
			createdConfigMap := &v1.ConfigMap{}
			configMapName := fmt.Sprintf("%s-druid-common-config", druidCR.Name)
			configMapNamespacedName := types.NamespacedName{Name: configMapName, Namespace: druidCR.Namespace}

			// Checking  the kubernetes service Type
			By(fmt.Sprintf("By checking common configmap for %s config ", configMapName))

			Eventually(func() bool {
				err := k8sClient.Get(ctx, configMapNamespacedName, createdConfigMap)
				return err == nil
			}, timeout, interval).Should(BeTrue())
			Expect(createdConfigMap.Data["metricDimensions.json"]).To(Equal(druidCR.Spec.DimensionsMapPath))
			Expect(createdConfigMap.Data["common.runtime.properties"]).To(Equal(druidCR.Spec.CommonRuntimeProperties))
		})
		for _, componentName := range druidComponents {
			componentName := componentName
			It(fmt.Sprintf("Test configMap for %s", componentName), func() {
				createdConfigMap := &v1.ConfigMap{}
				configMapName := fmt.Sprintf("druid-%s-%s-config", druidCR.Name, componentName)
				configMapNamespacedName := types.NamespacedName{Name: configMapName, Namespace: druidCR.Namespace}
				runtimeProperties := fmt.Sprintf("druid.port=%d\n%s", druidCR.Spec.Nodes[componentName].DruidPort, druidCR.Spec.Nodes[componentName].RuntimeProperties)
				jvmConfig := fmt.Sprintf("%s\n%s", druidCR.Spec.JvmOptions, druidCR.Spec.Nodes[componentName].ExtraJvmOptions)
				// Checking  the kubernetes service Type
				By(fmt.Sprintf("By checking configmap check for %s config", configMapName))
				Eventually(func() bool {
					err := k8sClient.Get(ctx, configMapNamespacedName, createdConfigMap)
					return err == nil
				}, timeout, interval).Should(BeTrue())
				Expect(createdConfigMap.Data["log4j2.xml"]).To(Equal(druidCR.Spec.Log4jConfig))
				Expect(createdConfigMap.Data["runtime.properties"]).To(Equal(runtimeProperties))
				Expect(createdConfigMap.Data["jvm.config"]).To(Equal(jvmConfig))

			})
		}
		// Check for HPA
		It("Test horizontal pod autoscaler for broker", func() {
			componentName := "brokers"
			createdHpa := &autoscalev2.HorizontalPodAutoscaler{}
			hpaName := fmt.Sprintf("druid-%s-%s", druidCR.Name, componentName)
			hpaNamespacedName := types.NamespacedName{Name: hpaName, Namespace: druidCR.Namespace}

			// Checking  the kubernetes service Type
			By(fmt.Sprintf("By checking horizontal pod autoscaler  for %s ", hpaName))
			Eventually(func() bool {
				err := k8sClient.Get(ctx, hpaNamespacedName, createdHpa)
				return err == nil
			}, timeout, interval).Should(BeTrue())
			// check for max replicas
			Expect(createdHpa.Spec.MaxReplicas).To(Equal(druidCR.Spec.Nodes[componentName].HPAutoScaler.MaxReplicas))
			// check for min replicas
			Expect(createdHpa.Spec.MinReplicas).To(Equal(druidCR.Spec.Nodes[componentName].HPAutoScaler.MinReplicas))
			// check for ScaleTargetRef
			Expect(createdHpa.Spec.ScaleTargetRef).To(Equal(druidCR.Spec.Nodes[componentName].HPAutoScaler.ScaleTargetRef))
		})

	})
})

func areStringArraysEqual(a1, a2 []string) bool {
	if len(a1) == len(a2) {
		for i, v := range a1 {
			if v != a2[i] {
				return false
			}
		}
	} else {
		return false
	}
	return true
}

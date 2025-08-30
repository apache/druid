package druid

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	druidv1alpha1 "github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
)

// +kubebuilder:docs-gen:collapse=Imports

/*
util test
*/
var _ = Describe("Test util", func() {
	Context("When testing util", func() {
		It("should test first non nil value", func() {
			var js = []byte(`
			{
				"image": "apache/druid:25.0.0",
				"securityContext": { "fsGroup": 107, "runAsUser": 106 },
				"env": [{ "name": "k", "value": "v" }],
				"nodes":
				{
					"brokers": {
						"nodeType": "broker",
						"druid.port": 8080,
						"replicas": 2
					}
				}
			}`)

			clusterSpec := druidv1alpha1.DruidSpec{}
			Expect(json.Unmarshal(js, &clusterSpec)).Should(BeNil())

			By("By testing first non nil value of PodSecurityContext.RunAsUser")
			x := firstNonNilValue(clusterSpec.Nodes["brokers"].PodSecurityContext, clusterSpec.PodSecurityContext).(*v1.PodSecurityContext)
			Expect(*x.RunAsUser).Should(Equal(int64(106)))

			By("By testing first non nil value of Env.Name")
			y := firstNonNilValue(clusterSpec.Nodes["brokers"].Env, clusterSpec.Env).([]v1.EnvVar)
			Expect(y[0].Name).Should(Equal("k"))
		})

		It("should test first non empty string", func() {
			By("By testing first non empty string 1")
			Expect(firstNonEmptyStr("a", "b")).Should(Equal("a"))

			By("By testing first non empty string 2")
			Expect(firstNonEmptyStr("", "b")).Should(Equal("b"))
		})

		It("should test contains string", func() {
			By("By testing contains string")
			Expect(ContainsString([]string{"a", "b"}, "a")).Should(BeTrue())
		})

		It("should test removes string", func() {
			By("By testing removes string")
			rs := RemoveString([]string{"a", "b"}, "a")
			Expect(rs).Should(Not(ConsistOf("a")))
		})

	})
})

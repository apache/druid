package druid

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	druidv1alpha1 "github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
)

// +kubebuilder:docs-gen:collapse=Imports

/*
zookeeper_dep_mgmt_test
*/
var _ = Describe("Test zookeeper dep mgmt", func() {
	Context("When testing zookeeper dep mgmt", func() {
		It("should test zookeeper dep mgmt", func() {
			v := druidv1alpha1.ZookeeperSpec{
				Type: "default",
				Spec: []byte(`{ "properties": "my-zookeeper-config" }`),
			}

			zm, err := createZookeeperManager(&v)
			Expect(err).Should(BeNil())
			Expect(zm.Configuration()).Should(Equal("my-zookeeper-config"))

		})
	})
})

package druid

import "github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"

var (
	druidServicesOrder = []string{historical, overlord, middleManager, indexer, broker, coordinator, router}
)

type ServiceGroup struct {
	key  string
	spec v1alpha1.DruidNodeSpec
}

// getNodeSpecsByOrder returns all NodeSpecs f a given Druid object.
// Recommended order is described at http://druid.io/docs/latest/operations/rolling-updates.html
func getNodeSpecsByOrder(m *v1alpha1.Druid) []*ServiceGroup {

	scaledServiceSpecsByNodeType := map[string][]*ServiceGroup{}
	for _, t := range druidServicesOrder {
		scaledServiceSpecsByNodeType[t] = []*ServiceGroup{}
	}

	for key, nodeSpec := range m.Spec.Nodes {
		scaledServiceSpec := scaledServiceSpecsByNodeType[nodeSpec.NodeType]
		scaledServiceSpecsByNodeType[nodeSpec.NodeType] = append(scaledServiceSpec, &ServiceGroup{key: key, spec: nodeSpec})
	}

	allScaledServiceSpecs := make([]*ServiceGroup, 0, len(m.Spec.Nodes))

	for _, t := range druidServicesOrder {
		allScaledServiceSpecs = append(allScaledServiceSpecs, scaledServiceSpecsByNodeType[t]...)
	}

	return allScaledServiceSpecs
}

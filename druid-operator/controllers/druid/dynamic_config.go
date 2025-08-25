package druid

import (
	"context"
	"fmt"
	"net/http"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	druidapi "github.com/datainfrahq/druid-operator/pkg/druidapi"
	internalhttp "github.com/datainfrahq/druid-operator/pkg/http"
	"github.com/datainfrahq/druid-operator/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// updateDruidDynamicConfigs updates the Druid cluster's dynamic configurations
// for both overlords (middlemanagers) and coordinators.
func updateDruidDynamicConfigs(
	ctx context.Context,
	client client.Client,
	druid *v1alpha1.Druid,
	emitEvent EventEmitter,
) error {
	nodeTypes := []string{"middlemanagers", "coordinators"}

	for _, nodeType := range nodeTypes {
		nodeConfig, exists := druid.Spec.Nodes[nodeType]
		if !exists || nodeConfig.DynamicConfig.Size() == 0 {
			// Skip if dynamic configurations are not provided for the node type
			continue
		}

		dynamicConfig := nodeConfig.DynamicConfig.Raw

		svcName, err := druidapi.GetRouterSvcUrl(druid.Namespace, druid.Name, client)
		if err != nil {
			emitEvent.EmitEventGeneric(
				druid,
				string(druidGetRouterSvcUrlFailed),
				fmt.Sprintf("Failed to get router service URL for %s", nodeType),
				err,
			)
			return err
		}

		basicAuth, err := druidapi.GetAuthCreds(
			ctx,
			client,
			druid.Spec.Auth,
		)
		if err != nil {
			emitEvent.EmitEventGeneric(
				druid,
				string(druidGetAuthCredsFailed),
				fmt.Sprintf("Failed to get authentication credentials for %s", nodeType),
				err,
			)
			return err
		}

		// Create the HTTP client with basic authentication
		httpClient := internalhttp.NewHTTPClient(
			&http.Client{},
			&internalhttp.Auth{BasicAuth: basicAuth},
		)

		// Determine the URL path for dynamic configurations based on the nodeType
		var dynamicConfigPath string
		switch nodeType {
		case "middlemanagers":
			dynamicConfigPath = druidapi.MakePath(svcName, "indexer", "worker")
		case "coordinators":
			dynamicConfigPath = druidapi.MakePath(svcName, "coordinator", "config")
		default:
			return fmt.Errorf("unsupported node type: %s", nodeType)
		}

		// Fetch current dynamic configurations
		currentResp, err := httpClient.Do(
			http.MethodGet,
			dynamicConfigPath,
			nil,
		)
		if err != nil {
			emitEvent.EmitEventGeneric(
				druid,
				string(druidFetchCurrentConfigsFailed),
				fmt.Sprintf("Failed to fetch current %s dynamic configurations", nodeType),
				err,
			)
			return err
		}
		if currentResp.StatusCode != http.StatusOK {
			err = fmt.Errorf(
				"failed to retrieve current Druid %s dynamic configurations. Status code: %d, Response body: %s",
				nodeType, currentResp.StatusCode, string(currentResp.ResponseBody),
			)
			emitEvent.EmitEventGeneric(
				druid,
				string(druidFetchCurrentConfigsFailed),
				fmt.Sprintf("Failed to fetch current %s dynamic configurations", nodeType),
				err,
			)
			return err
		}

		// Handle empty response body
		var currentConfigsJson string
		if len(currentResp.ResponseBody) == 0 {
			currentConfigsJson = "{}" // Initialize as empty JSON object if response body is empty
		} else {
			currentConfigsJson = currentResp.ResponseBody
		}

		// Compare current and desired configurations
		equal, err := util.IncludesJson(currentConfigsJson, string(dynamicConfig))
		if err != nil {
			emitEvent.EmitEventGeneric(
				druid,
				string(druidConfigComparisonFailed),
				fmt.Sprintf("Failed to compare %s configurations", nodeType),
				err,
			)
			return err
		}
		if equal {
			// Configurations are already up-to-date
			continue
		}

		// Update the Druid cluster's dynamic configurations if needed
		respDynamicConfigs, err := httpClient.Do(
			http.MethodPost,
			dynamicConfigPath,
			dynamicConfig,
		)
		if err != nil {
			emitEvent.EmitEventGeneric(
				druid,
				string(druidUpdateConfigsFailed),
				fmt.Sprintf("Failed to update %s dynamic configurations", nodeType),
				err,
			)
			return err
		}
		if respDynamicConfigs.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to update Druid %s dynamic configurations", nodeType)
		}

		emitEvent.EmitEventGeneric(
			druid,
			string(druidUpdateConfigsSuccess),
			fmt.Sprintf("Successfully updated %s dynamic configurations", nodeType),
			nil,
		)
	}

	return nil
}

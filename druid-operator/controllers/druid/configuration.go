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
	"context"
	"fmt"

	"github.com/datainfrahq/druid-operator/apis/druid/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func makeConfigMap(name string, namespace string, labels map[string]string, data map[string]string) (*v1.ConfigMap, error) {
	return &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Data: data,
	}, nil
}

func makeCommonConfigMap(ctx context.Context, sdk client.Client, m *v1alpha1.Druid, ls map[string]string) (*v1.ConfigMap, error) {
	prop := m.Spec.CommonRuntimeProperties

	if m.Spec.Zookeeper != nil {
		if zm, err := createZookeeperManager(m.Spec.Zookeeper); err != nil {
			return nil, err
		} else {
			prop = prop + "\n" + zm.Configuration() + "\n"
		}
	}

	if m.Spec.MetadataStore != nil {
		if msm, err := createMetadataStoreManager(m.Spec.MetadataStore); err != nil {
			return nil, err
		} else {
			prop = prop + "\n" + msm.Configuration() + "\n"
		}
	}

	if m.Spec.DeepStorage != nil {
		if dsm, err := createDeepStorageManager(m.Spec.DeepStorage); err != nil {
			return nil, err
		} else {
			prop = prop + "\n" + dsm.Configuration() + "\n"
		}
	}

	data := map[string]string{
		"common.runtime.properties": prop,
	}

	if m.Spec.DimensionsMapPath != "" {
		data["metricDimensions.json"] = m.Spec.DimensionsMapPath
	}
	if m.Spec.HdfsSite != "" {
		data["hdfs-site.xml"] = m.Spec.HdfsSite
	}
	if m.Spec.CoreSite != "" {
		data["core-site.xml"] = m.Spec.CoreSite
	}

	if err := addExtraCommonConfig(ctx, sdk, m, data); err != nil {
		return nil, err
	}

	cfg, err := makeConfigMap(
		fmt.Sprintf("%s-druid-common-config", m.ObjectMeta.Name),
		m.Namespace,
		ls,
		data)
	return cfg, err
}

func addExtraCommonConfig(ctx context.Context, sdk client.Client, m *v1alpha1.Druid, data map[string]string) error {
	if m.Spec.ExtraCommonConfig == nil {
		return nil
	}

	for _, cmRef := range m.Spec.ExtraCommonConfig {
		cm := &v1.ConfigMap{}
		if err := sdk.Get(ctx, types.NamespacedName{
			Name:      cmRef.Name,
			Namespace: cmRef.Namespace}, cm); err != nil {
			// If a configMap is not found - output error and keep reconciliation
			continue
		}

		for fileName, fileContent := range cm.Data {
			data[fileName] = fileContent
		}
	}

	return nil
}

func makeConfigMapForNodeSpec(nodeSpec *v1alpha1.DruidNodeSpec, m *v1alpha1.Druid, lm map[string]string, nodeSpecUniqueStr string) (*v1.ConfigMap, error) {

	data := map[string]string{
		"runtime.properties": fmt.Sprintf("druid.port=%d\n%s", nodeSpec.DruidPort, nodeSpec.RuntimeProperties),
		"jvm.config":         fmt.Sprintf("%s\n%s", firstNonEmptyStr(nodeSpec.JvmOptions, m.Spec.JvmOptions), nodeSpec.ExtraJvmOptions),
	}
	log4jconfig := firstNonEmptyStr(nodeSpec.Log4jConfig, m.Spec.Log4jConfig)
	if log4jconfig != "" {
		data["log4j2.xml"] = log4jconfig
	}

	return makeConfigMap(
		fmt.Sprintf("%s-config", nodeSpecUniqueStr),
		m.Namespace,
		lm,
		data)
}

func getNodeConfigMountPath(nodeSpec *v1alpha1.DruidNodeSpec) string {
	return fmt.Sprintf("/druid/conf/druid/%s", nodeSpec.NodeType)
}

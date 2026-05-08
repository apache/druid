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

package org.apache.druid.k8s.overlord;

import org.apache.druid.java.util.common.RE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class MultipleKubernetesTaskRunnerConfigTest
{
  @Test
  public void test_fromProperties_withSingleCluster()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    props.setProperty("druid.indexer.runner.clusters[0].kubeconfigPath", "/path/to/kubeconfig1");
    props.setProperty("druid.indexer.runner.clusters[0].overlordIdentifier", "overlord1");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getClusters());
    Assertions.assertEquals(1, config.getClusters().size());

    MultipleKubernetesTaskRunnerConfig.KubernetesCluster cluster = config.getClusters().get(0);
    Assertions.assertEquals("namespace1", cluster.getTaskNamespace());
    Assertions.assertEquals("/path/to/kubeconfig1", cluster.getKubeconfigPath());
    Assertions.assertEquals("overlord1", cluster.getOverlordIdentifier());

    // Should default to RoundRobinSelectionStrategy
    Assertions.assertInstanceOf(MultipleKubernetesTaskRunner.RoundRobinSelector.class, config.getClusterSelector());
  }

  @Test
  public void test_fromProperties_withMultipleClusters()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    props.setProperty("druid.indexer.runner.clusters[0].kubeconfigPath", "/path/to/kubeconfig1");
    props.setProperty("druid.indexer.runner.clusters[0].overlordIdentifier", "overlord1");

    props.setProperty("druid.indexer.runner.clusters[1].taskNamespace", "namespace2");
    props.setProperty("druid.indexer.runner.clusters[1].kubeconfigPath", "/path/to/kubeconfig2");
    props.setProperty("druid.indexer.runner.clusters[1].overlordIdentifier", "overlord2");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getClusters());
    Assertions.assertEquals(2, config.getClusters().size());

    MultipleKubernetesTaskRunnerConfig.KubernetesCluster cluster1 = config.getClusters().get(0);
    Assertions.assertEquals("namespace1", cluster1.getTaskNamespace());
    Assertions.assertEquals("/path/to/kubeconfig1", cluster1.getKubeconfigPath());
    Assertions.assertEquals("overlord1", cluster1.getOverlordIdentifier());

    MultipleKubernetesTaskRunnerConfig.KubernetesCluster cluster2 = config.getClusters().get(1);
    Assertions.assertEquals("namespace2", cluster2.getTaskNamespace());
    Assertions.assertEquals("/path/to/kubeconfig2", cluster2.getKubeconfigPath());
    Assertions.assertEquals("overlord2", cluster2.getOverlordIdentifier());
  }

  @Test
  public void test_fromProperties_withNullableFields()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    // kubeconfigPath and overlordIdentifier are optional, so we don't set them

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getClusters());
    Assertions.assertEquals(1, config.getClusters().size());

    MultipleKubernetesTaskRunnerConfig.KubernetesCluster cluster = config.getClusters().get(0);
    Assertions.assertEquals("namespace1", cluster.getTaskNamespace());
    Assertions.assertNull(cluster.getKubeconfigPath());
    Assertions.assertNull(cluster.getOverlordIdentifier());
  }

  @Test
  public void test_fromProperties_withSelectionStrategy_roundrobin()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    props.setProperty("druid.indexer.runner.selectionStrategy.type", "roundrobin");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertInstanceOf(MultipleKubernetesTaskRunner.RoundRobinSelector.class, config.getClusterSelector());
  }

  @Test
  public void test_fromProperties_withSelectionStrategy_random()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    props.setProperty("druid.indexer.runner.clusterSelector.type", "random");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertInstanceOf(MultipleKubernetesTaskRunner.RandomSelector.class, config.getClusterSelector());
  }

  @Test
  public void test_fromProperties_withLabels_simpleKeys()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    // Test with simple label keys using dot notation
    props.setProperty("druid.indexer.runner.labels.simpleKey", "simpleValue");
    props.setProperty("druid.indexer.runner.labels.anotherKey", "anotherValue");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getLabels());
    Assertions.assertEquals(2, config.getLabels().size());
    Assertions.assertEquals("simpleValue", config.getLabels().get("simpleKey"));
    Assertions.assertEquals("anotherValue", config.getLabels().get("anotherKey"));
  }

  @Test
  public void test_fromProperties_withLabels_jsonFormat()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    // Test with JSON format for labels - supports complex keys with dots and special characters
    props.setProperty("druid.indexer.runner.labels", "{\"lxcfs-admission-webhook.k8s.io/no-mutating\":\"\",\"another.key.with.dots\":\"value\"}");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getLabels());
    Assertions.assertEquals(2, config.getLabels().size());
    Assertions.assertEquals("", config.getLabels().get("lxcfs-admission-webhook.k8s.io/no-mutating"));
    Assertions.assertEquals("value", config.getLabels().get("another.key.with.dots"));
  }

  @Test
  public void test_fromProperties_withAnnotations_jsonFormat()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    // Test with JSON format for annotations
    props.setProperty("druid.indexer.runner.annotations", "{\"prometheus.io/scrape\":\"true\",\"prometheus.io/port\":\"8080\"}");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getAnnotations());
    Assertions.assertEquals(2, config.getAnnotations().size());
    Assertions.assertEquals("true", config.getAnnotations().get("prometheus.io/scrape"));
    Assertions.assertEquals("8080", config.getAnnotations().get("prometheus.io/port"));
  }

  @Test
  public void test_fromProperties_withLabels_mixedJsonAndDotNotation()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    // Mix JSON format for complex keys
    props.setProperty("druid.indexer.runner.labels", "{\"lxcfs-admission-webhook.k8s.io/no-mutating\":\"\"}");
    // And dot notation for simple keys
    props.setProperty("druid.indexer.runner.labels.simpleKey", "simpleValue");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getLabels());
    Assertions.assertEquals(2, config.getLabels().size());
    Assertions.assertEquals("", config.getLabels().get("lxcfs-admission-webhook.k8s.io/no-mutating"));
    Assertions.assertEquals("simpleValue", config.getLabels().get("simpleKey"));
  }

  @Test
  public void test_fromProperties_withSelectionStrategy_leastTask()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    props.setProperty("druid.indexer.runner.clusterSelector.type", "leastTask");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertInstanceOf(MultipleKubernetesTaskRunner.LeastTaskSelector.class, config.getClusterSelector());
  }

  @Test
  public void test_fromProperties_withoutSelectionStrategy_defaultsToRoundRobin()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    // No selectionStrategy specified

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertInstanceOf(MultipleKubernetesTaskRunner.RoundRobinSelector.class, config.getClusterSelector());
  }

  @Test
  public void test_fromProperties_ignoresPropertiesWithoutPrefix()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");
    props.setProperty("druid.indexer.runner.k8s.someOtherProperty", "shouldBeIgnored");
    props.setProperty("some.other.property", "shouldAlsoBeIgnored");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getClusters());
    Assertions.assertEquals(1, config.getClusters().size());
    Assertions.assertEquals("namespace1", config.getClusters().get(0).getTaskNamespace());
  }

  @Test
  public void test_fromProperties_withEmptyProperties_throwsException()
  {
    final Properties props = new Properties();
    // No properties with the prefix - should throw exception due to validation

    Assertions.assertThrows(
        RE.class,
        () -> MultipleKubernetesTaskRunnerConfig.fromProperties(props)
    );
  }

  @Test
  public void test_fromProperties_withDotNotation()
  {
    Properties props = new Properties();
    // Test with dot notation instead of bracket notation
    props.setProperty("druid.indexer.runner.clusters.0.taskNamespace", "namespace1");
    props.setProperty("druid.indexer.runner.clusters.0.kubeconfigPath", "/path/to/kubeconfig1");
    props.setProperty("druid.indexer.runner.clusters.0.overlordIdentifier", "overlord1");

    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getClusters());
    Assertions.assertEquals(1, config.getClusters().size());

    MultipleKubernetesTaskRunnerConfig.KubernetesCluster cluster = config.getClusters().get(0);
    Assertions.assertEquals("namespace1", cluster.getTaskNamespace());
    Assertions.assertEquals("/path/to/kubeconfig1", cluster.getKubeconfigPath());
    Assertions.assertEquals("overlord1", cluster.getOverlordIdentifier());

    // Should default to RoundRobinSelectionStrategy
    Assertions.assertInstanceOf(MultipleKubernetesTaskRunner.RoundRobinSelector.class, config.getClusterSelector());
  }

  @Test
  public void test_fromProperties_withUnknownProperties_ignored()
  {
    Properties props = new Properties();
    // Set an unknown property - should be ignored due to FAIL_ON_UNKNOWN_PROPERTIES = false
    props.setProperty("druid.indexer.runner.unknownProperty", "someValue");
    props.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace1");

    // Should not throw an exception due to FAIL_ON_UNKNOWN_PROPERTIES = false
    MultipleKubernetesTaskRunnerConfig config = MultipleKubernetesTaskRunnerConfig.fromProperties(props);

    Assertions.assertNotNull(config);
    Assertions.assertNotNull(config.getClusters());
    Assertions.assertEquals(1, config.getClusters().size());
    Assertions.assertEquals("namespace1", config.getClusters().get(0).getTaskNamespace());
  }
}

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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.k8s.overlord.common.CachingKubernetesPeonClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesCachingClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.k8s.overlord.common.httpclient.vertx.DruidKubernetesVertxHttpClientConfig;
import org.apache.druid.k8s.overlord.common.httpclient.vertx.DruidKubernetesVertxHttpClientFactory;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.taskadapter.K8sTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.MultiContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateTaskAdapter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class MultipleKubernetesTaskRunnerFactoryTest
{
  @Test
  public void test_build_withSharedInformers_usesCachingPeonClient()
  {
    final TestMultipleKubernetesTaskRunnerFactory factory = createFactory(true);

    final TaskRunner taskRunner = factory.build();

    final KubernetesPeonClient peonClient = getOnlyPeonClient(taskRunner);
    Assertions.assertInstanceOf(CachingKubernetesPeonClient.class, peonClient);
    Assertions.assertEquals(1, factory.getCachingClients().size());

    ((MultipleKubernetesTaskRunner) taskRunner).stop();
  }

  @Test
  public void test_build_withoutSharedInformers_usesPlainPeonClient()
  {
    final TestMultipleKubernetesTaskRunnerFactory factory = createFactory(false);

    final TaskRunner taskRunner = factory.build();

    final KubernetesPeonClient peonClient = getOnlyPeonClient(taskRunner);
    Assertions.assertEquals(KubernetesPeonClient.class, peonClient.getClass());
    Assertions.assertTrue(factory.getCachingClients().isEmpty());

    ((MultipleKubernetesTaskRunner) taskRunner).stop();
  }

  @Test
  public void test_build_withDefaultAdapter_usesOverlordPodSourceClient()
  {
    final TestMultipleKubernetesTaskRunnerFactory factory = createFactory(false);

    final TaskRunner taskRunner = factory.build();
    final MultipleKubernetesTaskRunner multipleRunner = (MultipleKubernetesTaskRunner) taskRunner;
    final MultipleKubernetesTaskRunnerDelegate delegate = multipleRunner.getTaskRunners().get(0);
    final K8sTaskAdapter adapter = (K8sTaskAdapter) delegate.getRunner().adapter;

    Assertions.assertSame(factory.getOverlordPodSourceClient(), adapter.getPodSourceClient());
    Assertions.assertEquals("local-overlord-namespace", adapter.getPodSourceNamespace());
    Assertions.assertNotSame(factory.getOverlordPodSourceClient(), delegate.getKubernetesClient());

    multipleRunner.stop();
  }

  @Test
  public void test_build_withSidecarSupport_usesOverlordPodSourceClient()
  {
    final TestMultipleKubernetesTaskRunnerFactory factory = createFactory(
        false,
        properties -> properties.setProperty("druid.indexer.runner.sidecarSupport", "true")
    );

    final TaskRunner taskRunner = factory.build();
    final MultipleKubernetesTaskRunner multipleRunner = (MultipleKubernetesTaskRunner) taskRunner;
    final K8sTaskAdapter adapter = (K8sTaskAdapter) multipleRunner.getTaskRunners().get(0).getRunner().adapter;

    Assertions.assertInstanceOf(MultiContainerTaskAdapter.class, adapter);
    Assertions.assertSame(factory.getOverlordPodSourceClient(), adapter.getPodSourceClient());
    Assertions.assertEquals("local-overlord-namespace", adapter.getPodSourceNamespace());

    multipleRunner.stop();
  }

  @Test
  public void test_build_withCustomTemplateAdapter_doesNotCreateOverlordPodSourceClient()
  {
    final URL url = this.getClass().getClassLoader().getResource("basePodTemplate.yaml");
    final TestMultipleKubernetesTaskRunnerFactory factory = createFactory(false, properties -> {
      properties.setProperty("druid.indexer.runner.k8s.adapter.type", PodTemplateTaskAdapter.TYPE);
      properties.setProperty("druid.indexer.runner.k8s.podTemplate.base", url.getPath());
      properties.remove("druid.indexer.runner.overlordNamespace");
      properties.remove("druid.indexer.runner.namespace");
    });

    final TaskRunner taskRunner = factory.build();
    final MultipleKubernetesTaskRunner multipleRunner = (MultipleKubernetesTaskRunner) taskRunner;

    Assertions.assertInstanceOf(PodTemplateTaskAdapter.class, multipleRunner.getTaskRunners().get(0).getRunner().adapter);
    Assertions.assertNull(factory.getOverlordPodSourceClient());

    multipleRunner.stop();
  }

  @Test
  public void test_build_withDefaultAdapterAndSingleDefaultCluster_fallsBackToTaskNamespace()
  {
    final TestMultipleKubernetesTaskRunnerFactory factory = createFactory(false, properties -> {
      properties.remove("druid.indexer.runner.overlordNamespace");
      properties.remove("druid.indexer.runner.namespace");
    });

    final TaskRunner taskRunner = factory.build();
    final MultipleKubernetesTaskRunner multipleRunner = (MultipleKubernetesTaskRunner) taskRunner;
    final K8sTaskAdapter adapter = (K8sTaskAdapter) multipleRunner.getTaskRunners().get(0).getRunner().adapter;

    Assertions.assertEquals("namespace-a", adapter.getPodSourceNamespace());

    multipleRunner.stop();
  }

  @Test
  public void test_build_withDefaultAdapterAndAmbiguousOverlordPodSourceNamespace_throwsException()
  {
    final TestMultipleKubernetesTaskRunnerFactory factory = createFactory(false, properties -> {
      properties.remove("druid.indexer.runner.overlordNamespace");
      properties.remove("druid.indexer.runner.namespace");
      properties.setProperty("druid.indexer.runner.clusters[1].name", "cluster-b");
      properties.setProperty("druid.indexer.runner.clusters[1].taskNamespace", "namespace-b");
    });

    final IAE exception = Assertions.assertThrows(IAE.class, factory::build);
    Assertions.assertTrue(exception.getMessage().contains("druid.indexer.runner.overlordNamespace"));
    Assertions.assertTrue(exception.getMessage().contains("druid.indexer.runner.namespace"));
    Assertions.assertTrue(exception.getMessage().contains("customTemplateAdapter"));
  }

  private KubernetesPeonClient getOnlyPeonClient(TaskRunner taskRunner)
  {
    final MultipleKubernetesTaskRunner multipleRunner = (MultipleKubernetesTaskRunner) taskRunner;
    Assertions.assertEquals(1, multipleRunner.getTaskRunners().size());
    return multipleRunner.getTaskRunners().get(0).getRunner().getPeonClient();
  }

  private TestMultipleKubernetesTaskRunnerFactory createFactory(boolean useSharedInformers)
  {
    return createFactory(useSharedInformers, properties -> {});
  }

  private TestMultipleKubernetesTaskRunnerFactory createFactory(
      boolean useSharedInformers,
      Consumer<Properties> propertiesCustomizer
  )
  {
    final ObjectMapper objectMapper = new TestUtils().getTestObjectMapper();
    final Properties properties = new Properties();
    properties.setProperty("druid.indexer.runner.overlordNamespace", "local-overlord-namespace");
    properties.setProperty("druid.indexer.runner.clusters[0].name", "cluster-a");
    properties.setProperty("druid.indexer.runner.clusters[0].taskNamespace", "namespace-a");
    properties.setProperty("druid.indexer.runner.clusters[0].overlordIdentifier", "overlord-a");
    properties.setProperty("druid.indexer.runner.useK8sSharedInformers", Boolean.toString(useSharedInformers));
    propertiesCustomizer.accept(properties);

    final ConfigManager configManager = EasyMock.createNiceMock(ConfigManager.class);
    final HttpClient httpClient = EasyMock.createNiceMock(HttpClient.class);
    EasyMock.replay(configManager, httpClient);

    return new TestMultipleKubernetesTaskRunnerFactory(
        objectMapper,
        objectMapper,
        httpClient,
        new NoopTaskLogs(),
        properties,
        new NoopServiceEmitter(),
        () -> null,
        configManager,
        new TaskConfig(null, null, false, null, null, null, false, null, false, null, false, null),
        new StartupLoggingConfig(),
        new DruidNode("test-overlord", "localhost", false, 8080, null, true, false),
        new DruidKubernetesVertxHttpClientFactory(new DruidKubernetesVertxHttpClientConfig(), objectMapper)
    );
  }

  private static class TestMultipleKubernetesTaskRunnerFactory extends MultipleKubernetesTaskRunnerFactory
  {
    private final List<DruidKubernetesCachingClient> cachingClients = new ArrayList<>();
    private DruidKubernetesClient overlordPodSourceClient;

    TestMultipleKubernetesTaskRunnerFactory(
        ObjectMapper objectMapper,
        ObjectMapper smileMapper,
        HttpClient httpClient,
        NoopTaskLogs taskLogs,
        Properties properties,
        NoopServiceEmitter emitter,
        com.google.common.base.Supplier<KubernetesTaskRunnerDynamicConfig> dynamicConfigSupplier,
        ConfigManager configManager,
        TaskConfig taskConfig,
        StartupLoggingConfig startupLoggingConfig,
        DruidNode druidNode,
        DruidKubernetesVertxHttpClientFactory httpClientFactory
    )
    {
      super(
          objectMapper,
          smileMapper,
          httpClient,
          taskLogs,
          properties,
          emitter,
          dynamicConfigSupplier,
          configManager,
          taskConfig,
          startupLoggingConfig,
          druidNode,
          httpClientFactory
      );
    }

    @Override
    protected DruidKubernetesCachingClient createCachingClient(
        DruidKubernetesClient client,
        KubernetesTaskRunnerEffectiveConfig effectiveConfig
    )
    {
      final DruidKubernetesCachingClient cachingClient = EasyMock.createMock(DruidKubernetesCachingClient.class);
      EasyMock.expect(cachingClient.getBaseClient()).andReturn(client).anyTimes();
      cachingClient.stop();
      EasyMock.expectLastCall().anyTimes();
      EasyMock.replay(cachingClient);
      cachingClients.add(cachingClient);
      return cachingClient;
    }

    @Override
    protected DruidKubernetesClient createOverlordPodSourceClient(String overlordPodSourceNamespace)
    {
      final KubernetesClient kubernetesClient = EasyMock.createMock(KubernetesClient.class);
      kubernetesClient.close();
      EasyMock.expectLastCall().anyTimes();
      EasyMock.replay(kubernetesClient);

      overlordPodSourceClient = EasyMock.createMock(DruidKubernetesClient.class);
      EasyMock.expect(overlordPodSourceClient.getClient()).andReturn(kubernetesClient).anyTimes();
      EasyMock.replay(overlordPodSourceClient);
      return overlordPodSourceClient;
    }

    List<DruidKubernetesCachingClient> getCachingClients()
    {
      return cachingClients;
    }

    DruidKubernetesClient getOverlordPodSourceClient()
    {
      return overlordPodSourceClient;
    }
  }
}

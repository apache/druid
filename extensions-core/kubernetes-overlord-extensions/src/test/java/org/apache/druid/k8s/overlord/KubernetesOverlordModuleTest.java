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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.google.inject.TypeLiteral;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.ConfigManagerConfig;
import org.apache.druid.guice.ConfigModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.IndexingServiceTaskLogsModule;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.common.httpclient.DruidKubernetesHttpClientFactory;
import org.apache.druid.k8s.overlord.common.httpclient.jdk.DruidKubernetesJdkHttpClientFactory;
import org.apache.druid.k8s.overlord.common.httpclient.okhttp.DruidKubernetesOkHttpHttpClientFactory;
import org.apache.druid.k8s.overlord.common.httpclient.vertx.DruidKubernetesVertxHttpClientFactory;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.taskadapter.MultiContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.SingleContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URL;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@ExtendWith(EasyMockExtension.class)
public class KubernetesOverlordModuleTest
{
  @Mock
  private ServiceEmitter serviceEmitter;
  @Mock
  private TaskConfig taskConfig;
  @Mock
  private HttpClient httpClient;
  @Mock
  private RemoteTaskRunnerFactory remoteTaskRunnerFactory;
  @Mock
  private HttpRemoteTaskRunnerFactory httpRemoteTaskRunnerFactory;
  @Mock
  private ConfigManagerConfig configManagerConfig;
  @Mock
  private MetadataStorageTablesConfig metadataStorageTablesConfig;
  @Mock
  private AuditManager auditManager;
  @Mock
  private MetadataStorageConnector metadataStorageConnector;
  @Mock
  private ConfigManager configManager;
  private Injector injector;

  @BeforeEach
  public void setUpConfigManagerMock()
  {
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.watchConfig(
        EasyMock.anyString(),
        EasyMock.anyObject()
    )).andReturn(new AtomicReference<>(null)).anyTimes();
    EasyMock.expect(configManager.addListener(
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY),
        EasyMock.anyString(),
        EasyMock.anyObject(Consumer.class)
    )).andReturn(true).anyTimes();
    EasyMock.replay(configManager);
  }

  @Test
  public void testDefaultHttpRemoteTaskRunnerFactoryBindSuccessfully()
  {
    injector = makeInjectorWithProperties(initializePropertes(false), false, true);
    KubernetesAndWorkerTaskRunnerFactory taskRunnerFactory = injector.getInstance(
        KubernetesAndWorkerTaskRunnerFactory.class);
    Assertions.assertNotNull(taskRunnerFactory);

    Assertions.assertNotNull(taskRunnerFactory.build());
  }

  @Test
  public void testRemoteTaskRunnerFactoryBindSuccessfully()
  {
    injector = makeInjectorWithProperties(initializePropertes(true), true, false);
    KubernetesAndWorkerTaskRunnerFactory taskRunnerFactory = injector.getInstance(
        KubernetesAndWorkerTaskRunnerFactory.class);
    Assertions.assertNotNull(taskRunnerFactory);

    Assertions.assertNotNull(taskRunnerFactory.build());
  }

  @Test
  public void testExceptionThrownIfNoTaskRunnerFactoryBind()
  {
    Assertions.assertThrows(ProvisionException.class, () -> {
      injector = makeInjectorWithProperties(initializePropertes(false), false, false);
      injector.getInstance(KubernetesAndWorkerTaskRunnerFactory.class);
    });
  }

  @Test
  public void test_build_withMultiContainerAdapterType_returnsWithMultiContainerTaskAdapter()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.adapter.type", "overlordMultiContainer");
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");

    injector = makeInjectorWithProperties(props, false, true);
    TaskAdapter taskAdapter = injector.getInstance(
        TaskAdapter.class);

    Assertions.assertNotNull(taskAdapter);
    Assertions.assertTrue(taskAdapter instanceof MultiContainerTaskAdapter);
  }

  @Test
  public void test_build_withSingleContainerAdapterType_returnsKubernetesTaskRunnerWithSingleContainerTaskAdapter()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.adapter.type", "overlordSingleContainer");
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    injector = makeInjectorWithProperties(props, false, true);
    TaskAdapter taskAdapter = injector.getInstance(
        TaskAdapter.class);

    Assertions.assertNotNull(taskAdapter);
    Assertions.assertTrue(taskAdapter instanceof SingleContainerTaskAdapter);
  }

  @Test
  public void test_build_withSingleContainerAdapterTypeAndSidecarSupport_throwsProvisionException()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.adapter.type", "overlordSingleContainer");
    props.setProperty("druid.indexer.runner.sidecarSupport", "true");
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    injector = makeInjectorWithProperties(props, false, true);

    Assertions.assertThrows(
        ProvisionException.class,
        () -> injector.getInstance(TaskAdapter.class),
        "Invalid pod adapter [overlordSingleContainer], only pod adapter [overlordMultiContainer] can be specified when sidecarSupport is enabled"
    );
  }

  @Test
  public void test_build_withSidecarSupport_returnsKubernetesTaskRunnerWithMultiContainerTaskAdapter()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.sidecarSupport", "true");
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    injector = makeInjectorWithProperties(props, false, true);


    TaskAdapter adapter = injector.getInstance(TaskAdapter.class);

    Assertions.assertNotNull(adapter);
    Assertions.assertTrue(adapter instanceof MultiContainerTaskAdapter);
  }

  @Test
  public void test_build_withoutSidecarSupport_returnsKubernetesTaskRunnerWithSingleContainerTaskAdapter()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.sidecarSupport", "false");
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    injector = makeInjectorWithProperties(props, false, true);

    TaskAdapter adapter = injector.getInstance(TaskAdapter.class);

    Assertions.assertNotNull(adapter);
    Assertions.assertTrue(adapter instanceof SingleContainerTaskAdapter);
  }

  @Test
  public void test_build_withPodTemplateAdapterType_returnsKubernetesTaskRunnerWithPodTemplateTaskAdapter()
  {
    URL url = this.getClass().getClassLoader().getResource("basePodTemplate.yaml");

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.adapter.type", "customTemplateAdapter");
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", url.getPath());
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    injector = makeInjectorWithProperties(props, false, true);


    TaskAdapter adapter = injector.getInstance(TaskAdapter.class);

    Assertions.assertNotNull(adapter);
    Assertions.assertTrue(adapter instanceof PodTemplateTaskAdapter);
  }

  @Test
  public void test_httpClientFactory_defaultsToVertx()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    // Don't set httpClientType - should default to vertx

    injector = makeInjectorWithProperties(props, false, true);
    DruidKubernetesHttpClientFactory factory = injector.getInstance(DruidKubernetesHttpClientFactory.class);

    Assertions.assertNotNull(factory);
    Assertions.assertTrue(factory instanceof DruidKubernetesVertxHttpClientFactory,
                     "Should default to Vertx HTTP client");
  }

  @Test
  public void test_httpClientFactory_okhttpSelection()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    props.setProperty("druid.indexer.runner.k8sAndWorker.http.httpClientType", "okhttp");

    injector = makeInjectorWithProperties(props, false, true);
    DruidKubernetesHttpClientFactory factory = injector.getInstance(DruidKubernetesHttpClientFactory.class);

    Assertions.assertNotNull(factory);
    Assertions.assertTrue(factory instanceof DruidKubernetesOkHttpHttpClientFactory,
                     "Should select OkHttp HTTP client");
  }

  @Test
  public void test_httpClientFactory_vertxExplicitSelection()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    props.setProperty("druid.indexer.runner.k8sAndWorker.http.httpClientType", "vertx");

    injector = makeInjectorWithProperties(props, false, true);
    DruidKubernetesHttpClientFactory factory = injector.getInstance(DruidKubernetesHttpClientFactory.class);

    Assertions.assertNotNull(factory);
    Assertions.assertTrue(factory instanceof DruidKubernetesVertxHttpClientFactory,
                     "Should explicitly select Vertx HTTP client");
  }

  @Test
  public void test_httpClientFactory_jdkSelection()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    props.setProperty("druid.indexer.runner.k8sAndWorker.http.httpClientType", "javaStandardHttp");

    injector = makeInjectorWithProperties(props, false, true);
    DruidKubernetesHttpClientFactory factory = injector.getInstance(DruidKubernetesHttpClientFactory.class);

    Assertions.assertNotNull(factory);
    Assertions.assertTrue(factory instanceof DruidKubernetesJdkHttpClientFactory,
                     "Should select JDK HTTP client");
  }

  @Test
  public void test_httpClientFactory_invalidTypeThrowsException()
  {
    Assertions.assertThrows(ProvisionException.class, () -> {
      Properties props = new Properties();
      props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
      props.setProperty("druid.indexer.runner.k8sAndWorker.http.httpClientType", "invalid");

      injector = makeInjectorWithProperties(props, false, true);
      injector.getInstance(DruidKubernetesHttpClientFactory.class);
    });
  }

  @Test
  public void test_druidKubernetesClient_createdWithVertxClient()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    // Don't set httpClientType - should default to vertx

    injector = makeInjectorWithProperties(props, false, true);
    DruidKubernetesClient client = injector.getInstance(DruidKubernetesClient.class);

    Assertions.assertNotNull(client, "DruidKubernetesClient should be created successfully");
    Assertions.assertNotNull(client.getClient(), "Underlying Kubernetes client should be created");
  }

  private Injector makeInjectorWithProperties(
      final Properties props,
      boolean isWorkerTypeRemote,
      boolean isWorkerTypeHttpRemote
  )
  {
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new JacksonModule(),

            binder -> {
              binder.bind(Properties.class).toInstance(props);
              binder.bind(ServiceEmitter.class).toInstance(serviceEmitter);
              binder.bind(HttpClient.class).annotatedWith(EscalatedGlobal.class).toInstance(httpClient);
              binder.bind(TaskConfig.class).toInstance(taskConfig);
              binder.bind(DruidNode.class)
                    .annotatedWith(Self.class)
                    .toInstance(new DruidNode("test-inject", null, false, null, null, true, false));
              if (isWorkerTypeRemote) {
                binder.bind(RemoteTaskRunnerFactory.class).toInstance(remoteTaskRunnerFactory);
              }
              if (isWorkerTypeHttpRemote) {
                binder.bind(HttpRemoteTaskRunnerFactory.class).toInstance(httpRemoteTaskRunnerFactory);
              }
              binder.bind(
                  new TypeLiteral<Supplier<ConfigManagerConfig>>()
                  {
                  }).toInstance(Suppliers.ofInstance(configManagerConfig));
              binder.bind(
                  new TypeLiteral<Supplier<MetadataStorageTablesConfig>>()
                  {
                  }).toInstance(Suppliers.ofInstance(metadataStorageTablesConfig));
              binder.bind(AuditManager.class).toInstance(auditManager);
              binder.bind(MetadataStorageConnector.class).toInstance(metadataStorageConnector);
              binder.bind(ConfigManager.class).toInstance(configManager);
            },
            new ConfigModule(),
            new IndexingServiceTaskLogsModule(props),
            new KubernetesOverlordModule()
        ));
  }

  private static Properties initializePropertes(boolean isWorkerTypeRemote)
  {
    final Properties props = new Properties();
    props.put("druid.indexer.runner.namespace", "NAMESPACE");
    props.put("druid.indexer.runner.k8sAndWorker.runnerStrategy.type", "k8s");
    if (isWorkerTypeRemote) {
      props.put("druid.indexer.runner.k8sAndWorker.runnerStrategy.workerType", "remote");
    }
    return props;
  }
}

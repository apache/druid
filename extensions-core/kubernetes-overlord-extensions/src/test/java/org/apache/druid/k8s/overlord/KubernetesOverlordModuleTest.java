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
import org.apache.druid.common.config.ConfigManagerConfig;
import org.apache.druid.guice.ConfigModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.k8s.overlord.taskadapter.MultiContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.SingleContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URL;
import java.util.Properties;

@RunWith(EasyMockRunner.class)
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
  private Injector injector;

  @Test
  public void testDefaultHttpRemoteTaskRunnerFactoryBindSuccessfully()
  {
    injector = makeInjectorWithProperties(initializePropertes(false), false, true);
    KubernetesAndWorkerTaskRunnerFactory taskRunnerFactory = injector.getInstance(
        KubernetesAndWorkerTaskRunnerFactory.class);
    Assert.assertNotNull(taskRunnerFactory);

    Assert.assertNotNull(taskRunnerFactory.build());
  }

  @Test
  public void testRemoteTaskRunnerFactoryBindSuccessfully()
  {
    injector = makeInjectorWithProperties(initializePropertes(true), true, false);
    KubernetesAndWorkerTaskRunnerFactory taskRunnerFactory = injector.getInstance(
        KubernetesAndWorkerTaskRunnerFactory.class);
    Assert.assertNotNull(taskRunnerFactory);

    Assert.assertNotNull(taskRunnerFactory.build());
  }

  @Test(expected = ProvisionException.class)
  public void testExceptionThrownIfNoTaskRunnerFactoryBind()
  {
    injector = makeInjectorWithProperties(initializePropertes(false), false, false);
    injector.getInstance(KubernetesAndWorkerTaskRunnerFactory.class);
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

    Assert.assertNotNull(taskAdapter);
    Assert.assertTrue(taskAdapter instanceof MultiContainerTaskAdapter);
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

    Assert.assertNotNull(taskAdapter);
    Assert.assertTrue(taskAdapter instanceof SingleContainerTaskAdapter);
  }

  @Test
  public void test_build_withSingleContainerAdapterTypeAndSidecarSupport_throwsProvisionException()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.adapter.type", "overlordSingleContainer");
    props.setProperty("druid.indexer.runner.sidecarSupport", "true");
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    injector = makeInjectorWithProperties(props, false, true);

    Assert.assertThrows(
        "Invalid pod adapter [overlordSingleContainer], only pod adapter [overlordMultiContainer] can be specified when sidecarSupport is enabled",
        ProvisionException.class,
        () -> injector.getInstance(TaskAdapter.class)
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

    Assert.assertNotNull(adapter);
    Assert.assertTrue(adapter instanceof MultiContainerTaskAdapter);
  }

  @Test
  public void test_build_withoutSidecarSupport_returnsKubernetesTaskRunnerWithSingleContainerTaskAdapter()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.sidecarSupport", "false");
    props.setProperty("druid.indexer.runner.namespace", "NAMESPACE");
    injector = makeInjectorWithProperties(props, false, true);


    TaskAdapter adapter = injector.getInstance(TaskAdapter.class);

    Assert.assertNotNull(adapter);
    Assert.assertTrue(adapter instanceof SingleContainerTaskAdapter);
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

    Assert.assertNotNull(adapter);
    Assert.assertTrue(adapter instanceof PodTemplateTaskAdapter);
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
            },
            new ConfigModule(),
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

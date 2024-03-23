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

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
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
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

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

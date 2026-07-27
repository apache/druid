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

import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.k8s.overlord.execution.DefaultKubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class AutoscalableThreadPoolExecutorTest
{
  @Test
  public void testConstructorWithNullConfigManager()
  {
    final AutoscalableThreadPoolExecutor executor = new AutoscalableThreadPoolExecutor(2, null);

    Assertions.assertEquals(2, executor.getCorePoolSize());
    executor.shutdownNow();
  }

  @Test
  public void testDynamicConfigWithNullCapacityDoesNotChangePoolSize()
  {
    final ConfigManager configManager = EasyMock.createMock(ConfigManager.class);
    final Capture<Consumer<KubernetesTaskRunnerDynamicConfig>> listenerCapture = EasyMock.newCapture();

    EasyMock.expect(configManager.addListener(
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY),
        EasyMock.anyString(),
        EasyMock.capture(listenerCapture)
    )).andReturn(true);
    EasyMock.expect(configManager.removeListener(
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY),
        EasyMock.anyString(),
        EasyMock.anyObject()
    )).andReturn(true).anyTimes();
    EasyMock.replay(configManager);

    final AutoscalableThreadPoolExecutor executor = new AutoscalableThreadPoolExecutor(2, configManager);
    listenerCapture.getValue().accept(new DefaultKubernetesTaskRunnerDynamicConfig(null, null));

    Assertions.assertEquals(2, executor.getCorePoolSize());
    Assertions.assertEquals(2, executor.getMaximumPoolSize());

    executor.shutdownNow();
    EasyMock.verify(configManager);
  }

  @Test
  public void testDynamicConfigWithCapacityChangesPoolSize() throws InterruptedException
  {
    final ConfigManager configManager = EasyMock.createMock(ConfigManager.class);
    final Capture<Consumer<KubernetesTaskRunnerDynamicConfig>> listenerCapture = EasyMock.newCapture();

    EasyMock.expect(configManager.addListener(
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY),
        EasyMock.anyString(),
        EasyMock.capture(listenerCapture)
    )).andReturn(true);
    EasyMock.expect(configManager.removeListener(
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY),
        EasyMock.anyString(),
        EasyMock.anyObject()
    )).andReturn(true).anyTimes();
    EasyMock.replay(configManager);

    final AutoscalableThreadPoolExecutor executor = new AutoscalableThreadPoolExecutor(2, configManager);
    listenerCapture.getValue().accept(new DefaultKubernetesTaskRunnerDynamicConfig(null, 4));

    Assertions.assertEquals(4, executor.getCorePoolSize());
    Assertions.assertEquals(4, executor.getMaximumPoolSize());

    executor.shutdown();
    Assertions.assertTrue(executor.isShutdown());
    Assertions.assertTrue(executor.getQueue().isEmpty());
    Assertions.assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
    EasyMock.verify(configManager);
  }
}

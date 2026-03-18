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
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class AutoscalableThreadPoolExecutor extends ThreadPoolExecutor
{
  private static final EmittingLogger log = new EmittingLogger(AutoscalableThreadPoolExecutor.class);

  private final ConfigManager configManager;
  private final String listenerKey;
  private final Consumer<KubernetesTaskRunnerDynamicConfig> configListener;

  public AutoscalableThreadPoolExecutor(int initialCapacity, ConfigManager configManager)
  {
    super(
        initialCapacity,
        initialCapacity,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        Execs.makeThreadFactory("k8s-task-runner-%d", null)
    );

    this.configManager = configManager;
    this.listenerKey = String.format("AutoscalableThreadPoolExecutor-%d", System.identityHashCode(this));
    this.configListener = this::onConfigurationChange;

    // Monitor the configuration change
    if (!configManager.addListener(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        listenerKey,
        configListener)) {
      log.error("Failed to add configuration listener for AutoscalableThreadPoolExecutor");
    }
  }

  @Override
  public void shutdown()
  {
    removeConfigListener();
    super.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    removeConfigListener();
    return super.shutdownNow();
  }

  private void removeConfigListener()
  {
    if (!configManager.removeListener(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        listenerKey,
        configListener
    )) {
      log.warn("Failed to remove configuration listener[%s] for AutoscalableThreadPoolExecutor", listenerKey);
    }
  }

  private void onConfigurationChange(KubernetesTaskRunnerDynamicConfig config)
  {
    int curCapacity = this.getCorePoolSize();
    int newCapacity = config.getCapacity();
    if (newCapacity == curCapacity) {
      return;
    }

    log.info("Adjusting k8s task runner capacity from [%d] to [%d]", curCapacity, newCapacity);
    // maximum pool size must always be greater than or equal to the core pool size
    if (newCapacity < curCapacity) {
      // decrease capacity
      setCorePoolSize(newCapacity);
      setMaximumPoolSize(newCapacity);
    } else {
      // increase capacity
      setMaximumPoolSize(newCapacity);
      setCorePoolSize(newCapacity);
    }
  }
}

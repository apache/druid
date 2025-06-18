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

package org.apache.druid.testing.simulate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.cli.CliOverlord;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.curator.ZkEnablementConfig;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningSchedulerConfig;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunner;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.utils.RuntimeInfo;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Embedded mode of {@link CliOverlord} used in simulation tests.
 */
public class EmbeddedOverlord extends EmbeddedDruidServer
{
  private static final Logger log = new Logger(EmbeddedOverlord.class);

  private static final Map<String, String> DEFAULT_PROPERTIES = Map.of(
      "druid.indexer.runner.type", "simulation",
      "druid.indexer.queue.startDelay", "PT0S",
      "druid.indexer.queue.restartDelay", "PT0S",
      // Keep a small sync timeout so that Peons and Indexers are not stuck
      // handling a change request when Overlord has already shutdown
      "druid.indexer.runner.syncRequestTimeout", "PT1S"
  );

  private final Map<String, String> overrideProperties;
  private final ReferenceHolder referenceHolder;
  private final TaskRunnerListener taskRunnerListener;
  private final ConcurrentHashMap<String, CountDownLatch> taskHasCompleted;

  public static EmbeddedOverlord create()
  {
    return withProps(Map.of());
  }

  public static EmbeddedOverlord withProps(
      Map<String, String> properties
  )
  {
    return new EmbeddedOverlord(properties);
  }

  private EmbeddedOverlord(Map<String, String> overrideProperties)
  {
    this.overrideProperties = overrideProperties;
    this.referenceHolder = new ReferenceHolder();
    this.taskHasCompleted = new ConcurrentHashMap<>();
    this.taskRunnerListener = new TaskRunnerListener()
    {
      @Override
      public String getListenerId()
      {
        return "EmbeddedOverlord.TaskRunnerListener";
      }

      @Override
      public void locationChanged(String taskId, TaskLocation newLocation)
      {

      }

      @Override
      public void statusChanged(String taskId, TaskStatus status)
      {
        log.info("Task[%s] has updated status[%s]", taskId, status);
        if (status.isComplete()) {
          taskHasCompleted.compute(
              taskId,
              (t, existingLatch) -> {
                final CountDownLatch latch = Objects.requireNonNullElse(
                    existingLatch,
                    new CountDownLatch(1)
                );
                latch.countDown();
                return latch;
              }
          );
        }
      }
    };
  }

  @Override
  ServerRunnable createRunnable(LifecycleInitHandler handler)
  {
    return new Overlord(handler);
  }

  @Override
  RuntimeInfo getRuntimeInfo()
  {
    final long mem1gb = 1_000_000_000;
    return new DruidProcessingConfigTest.MockRuntimeInfo(4, mem1gb, mem1gb);
  }

  @Override
  Properties buildStartupProperties(
      TestFolder testFolder,
      EmbeddedZookeeper zk,
      @Nullable TestDerbyConnector.DerbyConnectorRule dbRule
  ) throws IOException
  {
    final Properties properties = super.buildStartupProperties(testFolder, zk, dbRule);
    properties.putAll(DEFAULT_PROPERTIES);
    properties.putAll(overrideProperties);
    return properties;
  }

  /**
   * Client to communicate with the leader Overlord, which may not be the same
   * as this one.
   */
  public OverlordClient client()
  {
    return leaderOverlord();
  }

  /**
   * Metadata storage coordinator to query and update segment metadata directly
   * in the metadata store.
   */
  public IndexerMetadataStorageCoordinator segmentsMetadataStorage()
  {
    return referenceHolder.indexerMetadataStorageCoordinator;
  }

  public void waitUntilTaskFinishes(String taskId)
  {
    try {
      final CountDownLatch latch = taskHasCompleted.computeIfAbsent(taskId, t -> new CountDownLatch(1));
      if (!latch.await(30, TimeUnit.SECONDS)) {
        log.error("Timed out waiting for task[%s] to finish.", taskId);
      }
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Extends {@link CliOverlord} to for the following:
   * <ul>
   * <li>Get reference to lifecycle and other dependencies used by the server.</li>
   * <li>Override {@link HttpRemoteTaskRunnerFactory} to register a
   * {@link TaskRunnerListener} to get completion callbacks.</li>
   * </ul>
   */
  private class Overlord extends CliOverlord
  {
    private final LifecycleInitHandler handler;

    private Overlord(LifecycleInitHandler handler)
    {
      this.handler = handler;
    }

    @Override
    public Lifecycle initLifecycle(Injector injector)
    {
      final Lifecycle lifecycle = super.initLifecycle(injector);
      handler.onLifecycleInit(lifecycle);
      return lifecycle;
    }

    @Override
    protected List<? extends Module> getModules()
    {
      final List<Module> modules = new ArrayList<>(handler.getInitModules());
      modules.addAll(super.getModules());
      modules.add(
          binder -> binder.bind(ReferenceHolder.class).toInstance(referenceHolder)
      );

      // Override TaskRunnerFactory to register a TaskRunnerListener
      modules.add(
          binder -> binder.bind(TaskRunnerListener.class).toInstance(taskRunnerListener)
      );
      modules.add(
          binder -> PolyBind.optionBinder(binder, Key.get(TaskRunnerFactory.class))
                            .addBinding("simulation")
                            .to(TestHttpRemoteTaskRunnerFactory.class)
                            .in(LazySingleton.class)
      );
      return modules;
    }
  }

  /**
   * Wraps around {@link HttpRemoteTaskRunnerFactory} to be able to register the
   * {@link #taskRunnerListener} on the created {@link HttpRemoteTaskRunner}.
   */
  private static class TestHttpRemoteTaskRunnerFactory implements TaskRunnerFactory<HttpRemoteTaskRunner>
  {
    private final TaskRunnerListener listener;
    private final HttpRemoteTaskRunnerFactory delegate;

    @Inject
    public TestHttpRemoteTaskRunnerFactory(
        @Smile final ObjectMapper smileMapper,
        final HttpRemoteTaskRunnerConfig httpRemoteTaskRunnerConfig,
        @EscalatedGlobal final HttpClient httpClient,
        final Supplier<WorkerBehaviorConfig> workerConfigRef,
        final ProvisioningSchedulerConfig provisioningSchedulerConfig,
        final ProvisioningStrategy provisioningStrategy,
        final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
        final TaskStorage taskStorage,
        final Provider<CuratorFramework> cfProvider,
        final IndexerZkConfig indexerZkConfig,
        final ZkEnablementConfig zkEnablementConfig,
        final ServiceEmitter emitter,
        final TaskRunnerListener listener
    )
    {
      this.delegate = new HttpRemoteTaskRunnerFactory(
          smileMapper,
          httpRemoteTaskRunnerConfig, httpClient, workerConfigRef,
          provisioningSchedulerConfig, provisioningStrategy, druidNodeDiscoveryProvider,
          taskStorage, cfProvider, indexerZkConfig, zkEnablementConfig, emitter
      );
      this.listener = listener;
    }

    @Override
    public HttpRemoteTaskRunner build()
    {
      final HttpRemoteTaskRunner runner = delegate.build();
      runner.registerListener(listener, MoreExecutors.directExecutor());
      return runner;
    }

    @Override
    public HttpRemoteTaskRunner get()
    {
      return delegate.get();
    }
  }

  /**
   * Holder for references to various objects being used by this embedded Overlord.
   */
  private static class ReferenceHolder
  {
    @Inject
    IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  }
}

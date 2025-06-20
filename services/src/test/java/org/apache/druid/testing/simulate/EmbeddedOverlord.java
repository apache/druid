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

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.CliOverlord;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.indexing.common.task.TaskMetrics;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.utils.RuntimeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Embedded mode of {@link CliOverlord} used in simulation tests.
 */
public class EmbeddedOverlord extends EmbeddedDruidServer
{
  private static final Map<String, String> DEFAULT_PROPERTIES = Map.of(
      "druid.indexer.queue.startDelay", "PT0S",
      "druid.indexer.queue.restartDelay", "PT0S",
      // Keep a small sync timeout so that Peons and Indexers are not stuck
      // handling a change request when Overlord has already shutdown
      "druid.indexer.runner.syncRequestTimeout", "PT1S"
  );

  private final Map<String, String> overrideProperties;
  private final ReferenceHolder referenceHolder;

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
      EmbeddedZookeeper zk
  )
  {
    final Properties properties = super.buildStartupProperties(testFolder, zk);
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
    emitter().waitForEvent(
        event -> event.hasMetricName(TaskMetrics.RUN_DURATION)
                      .hasDimension(DruidMetrics.TASK_ID, taskId)
    );
  }

  /**
   * Extends {@link CliOverlord} to get the server lifecycle.
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

      return modules;
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

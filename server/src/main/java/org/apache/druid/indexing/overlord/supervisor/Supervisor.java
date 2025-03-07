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

package org.apache.druid.indexing.overlord.supervisor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.segment.incremental.ParseExceptionReport;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * An interface representing a general supervisor for managing ingestion tasks. For streaming ingestion use cases,
 * see {@link StreamSupervisor}.
 */
public interface Supervisor
{
  void start();

  /**
   * @param stopGracefully If true, supervisor will cleanly shutdown managed tasks if possible (for example signalling
   *                       them to publish their segments and exit). The implementation may block until the tasks have
   *                       either acknowledged or completed. If false, supervisor will stop immediately and leave any
   *                       running tasks as they are.
   */
  void stop(boolean stopGracefully);

  /**
   * Starts non-graceful shutdown of the supervisor and returns a future that completes when shutdown is complete.
   */
  default ListenableFuture<Void> stopAsync()
  {
    SettableFuture<Void> stopFuture = SettableFuture.create();
    try {
      stop(false);
      stopFuture.set(null);
    }
    catch (Exception e) {
      stopFuture.setException(e);
    }
    return stopFuture;
  }

  SupervisorReport getStatus();

  SupervisorStateManager.State getState();

  default Map<String, Map<String, Object>> getStats()
  {
    return ImmutableMap.of();
  }

  default List<ParseExceptionReport> getParseErrors()
  {
    return ImmutableList.of();
  }

  @Nullable
  default Boolean isHealthy()
  {
    return null; // default implementation for interface compatability; returning null since true or false is misleading
  }

  /**
   * Resets any stored metadata by the supervisor.
   * @param dataSourceMetadata optional dataSource metadata.
   */
  void reset(@Nullable DataSourceMetadata dataSourceMetadata);
}

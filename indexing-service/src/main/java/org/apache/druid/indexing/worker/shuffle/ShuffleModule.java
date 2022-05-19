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

package org.apache.druid.indexing.worker.shuffle;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.metrics.MonitorScheduler;

import java.util.Optional;

public class ShuffleModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    Jerseys.addResource(binder, ShuffleResource.class);
  }

  /**
   * {@link ShuffleMetrics} is used in {@link ShuffleResource} and {@link ShuffleMonitor} to collect metrics
   * and report them, respectively. Unlike ShuffleResource, ShuffleMonitor can be created via a user config
   * ({@link org.apache.druid.server.metrics.MonitorsConfig}) in potentially any node types, where it is not
   * possible to create ShuffleMetrics. This method checks the {@link MonitorScheduler} if ShuffleMonitor is
   * registered on it, and sets the proper ShuffleMetrics.
   */
  @Provides
  @LazySingleton
  public Optional<ShuffleMetrics> getShuffleMetrics(MonitorScheduler monitorScheduler)
  {
    // ShuffleMonitor cannot be registered dynamically, but can only via the static configuration (MonitorsConfig).
    // As a result, it is safe to check only one time if it is registered in MonitorScheduler.
    final Optional<ShuffleMonitor> maybeMonitor = monitorScheduler.findMonitor(ShuffleMonitor.class);
    if (maybeMonitor.isPresent()) {
      final ShuffleMetrics metrics = new ShuffleMetrics();
      maybeMonitor.get().setShuffleMetrics(metrics);
      return Optional.of(metrics);
    }
    return Optional.empty();
  }
}

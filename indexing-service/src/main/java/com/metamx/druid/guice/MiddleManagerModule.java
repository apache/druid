/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.druid.guice.annotations.Self;
import com.metamx.druid.indexing.coordinator.ForkingTaskRunner;
import com.metamx.druid.indexing.coordinator.TaskRunner;
import com.metamx.druid.indexing.worker.Worker;
import com.metamx.druid.indexing.worker.WorkerCuratorCoordinator;
import com.metamx.druid.indexing.worker.WorkerTaskMonitor;
import com.metamx.druid.indexing.worker.config.WorkerConfig;
import com.metamx.druid.initialization.DruidNode;

/**
 */
public class MiddleManagerModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);

    JsonConfigProvider.bind(binder, "druid.worker", WorkerConfig.class);

    binder.bind(TaskRunner.class).to(ForkingTaskRunner.class);
    binder.bind(ForkingTaskRunner.class).in(LazySingleton.class);

    binder.bind(WorkerTaskMonitor.class).in(ManageLifecycle.class);
    binder.bind(WorkerCuratorCoordinator.class).in(ManageLifecycle.class);
  }

  @Provides @LazySingleton
  public Worker getWorker(@Self DruidNode node, WorkerConfig config)
  {
    return new Worker(
        node.getHost(),
        config.getIp(),
        config.getCapacity(),
        config.getVersion()
    );
  }
}

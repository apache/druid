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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.tasklogs.SwitchingTaskLogStreamer;
import io.druid.indexing.common.tasklogs.TaskLogStreamer;
import io.druid.indexing.common.tasklogs.TaskLogs;
import io.druid.indexing.common.tasklogs.TaskRunnerTaskLogStreamer;
import io.druid.indexing.coordinator.DbTaskStorage;
import io.druid.indexing.coordinator.ForkingTaskRunnerFactory;
import io.druid.indexing.coordinator.HeapMemoryTaskStorage;
import io.druid.indexing.coordinator.IndexerDBCoordinator;
import io.druid.indexing.coordinator.RemoteTaskRunnerFactory;
import io.druid.indexing.coordinator.TaskLockbox;
import io.druid.indexing.coordinator.TaskMaster;
import io.druid.indexing.coordinator.TaskQueue;
import io.druid.indexing.coordinator.TaskRunnerFactory;
import io.druid.indexing.coordinator.TaskStorage;
import io.druid.indexing.coordinator.TaskStorageQueryAdapter;
import io.druid.indexing.coordinator.http.OverlordRedirectInfo;
import io.druid.indexing.coordinator.scaling.AutoScalingStrategy;
import io.druid.indexing.coordinator.scaling.EC2AutoScalingStrategy;
import io.druid.indexing.coordinator.scaling.NoopAutoScalingStrategy;
import io.druid.indexing.coordinator.scaling.ResourceManagementSchedulerConfig;
import io.druid.indexing.coordinator.scaling.ResourceManagementSchedulerFactory;
import io.druid.indexing.coordinator.scaling.ResourceManagementSchedulerFactoryImpl;
import io.druid.indexing.coordinator.scaling.ResourceManagementStrategy;
import io.druid.indexing.coordinator.scaling.SimpleResourceManagementConfig;
import io.druid.indexing.coordinator.scaling.SimpleResourceManagementStrategy;
import io.druid.indexing.coordinator.setup.WorkerSetupData;
import io.druid.server.http.RedirectFilter;
import io.druid.server.http.RedirectInfo;

import java.util.List;

/**
 */
public class OverlordModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(TaskMaster.class).in(ManageLifecycle.class);

    binder.bind(TaskLogStreamer.class).to(SwitchingTaskLogStreamer.class).in(LazySingleton.class);
    binder.bind(new TypeLiteral<List<TaskLogStreamer>>(){})
          .toProvider(
              new ListProvider<TaskLogStreamer>()
                  .add(TaskRunnerTaskLogStreamer.class)
                  .add(TaskLogs.class)
          )
          .in(LazySingleton.class);

    binder.bind(TaskActionClientFactory.class).to(LocalTaskActionClientFactory.class).in(LazySingleton.class);
    binder.bind(TaskActionToolbox.class).in(LazySingleton.class);
    binder.bind(TaskQueue.class).in(LazySingleton.class); // Lifecycle managed by TaskMaster instead
    binder.bind(IndexerDBCoordinator.class).in(LazySingleton.class);
    binder.bind(TaskLockbox.class).in(LazySingleton.class);
    binder.bind(TaskStorageQueryAdapter.class).in(LazySingleton.class);
    binder.bind(ResourceManagementSchedulerFactory.class)
          .to(ResourceManagementSchedulerFactoryImpl.class)
          .in(LazySingleton.class);

    configureTaskStorage(binder);
    configureRunners(binder);
    configureAutoscale(binder);

    binder.bind(RedirectFilter.class).in(LazySingleton.class);
    binder.bind(RedirectInfo.class).to(OverlordRedirectInfo.class).in(LazySingleton.class);
  }

  private void configureTaskStorage(Binder binder)
  {
    PolyBind.createChoice(
        binder, "druid.indexer.storage.type", Key.get(TaskStorage.class), Key.get(HeapMemoryTaskStorage.class)
    );
    final MapBinder<String, TaskStorage> storageBinder = PolyBind.optionBinder(binder, Key.get(TaskStorage.class));

    storageBinder.addBinding("local").to(HeapMemoryTaskStorage.class);
    binder.bind(HeapMemoryTaskStorage.class).in(LazySingleton.class);

    storageBinder.addBinding("db").to(DbTaskStorage.class);
    binder.bind(DbTaskStorage.class).in(LazySingleton.class);
  }

  private void configureRunners(Binder binder)
  {
    PolyBind.createChoice(
        binder, "druid.indexer.runner.type", Key.get(TaskRunnerFactory.class), Key.get(ForkingTaskRunnerFactory.class)
    );
    final MapBinder<String, TaskRunnerFactory> biddy = PolyBind.optionBinder(binder, Key.get(TaskRunnerFactory.class));

    IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);
    biddy.addBinding("local").to(ForkingTaskRunnerFactory.class);
    binder.bind(ForkingTaskRunnerFactory.class).in(LazySingleton.class);

    biddy.addBinding("remote").to(RemoteTaskRunnerFactory.class).in(LazySingleton.class);
    binder.bind(RemoteTaskRunnerFactory.class).in(LazySingleton.class);
  }

  private void configureAutoscale(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.indexer.autoscale", ResourceManagementSchedulerConfig.class);
    binder.bind(ResourceManagementStrategy.class).to(SimpleResourceManagementStrategy.class).in(LazySingleton.class);

    JacksonConfigProvider.bind(binder, WorkerSetupData.CONFIG_KEY, WorkerSetupData.class, null);

    PolyBind.createChoice(
        binder,
        "druid.indexer.autoscale.strategy",
        Key.get(AutoScalingStrategy.class),
        Key.get(NoopAutoScalingStrategy.class)
    );

    final MapBinder<String, AutoScalingStrategy> autoScalingBinder = PolyBind.optionBinder(
        binder, Key.get(AutoScalingStrategy.class)
    );
    autoScalingBinder.addBinding("ec2").to(EC2AutoScalingStrategy.class);
    binder.bind(EC2AutoScalingStrategy.class).in(LazySingleton.class);

    autoScalingBinder.addBinding("noop").to(NoopAutoScalingStrategy.class);
    binder.bind(NoopAutoScalingStrategy.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.indexer.autoscale", SimpleResourceManagementConfig.class);
  }
}

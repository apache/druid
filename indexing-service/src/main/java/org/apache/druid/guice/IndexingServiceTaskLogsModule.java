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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.druid.indexing.common.config.FileTaskLogsConfig;
import org.apache.druid.indexing.common.tasklogs.FileTaskLogs;
import org.apache.druid.indexing.common.tasklogs.SwitchingTaskLogs;
import org.apache.druid.indexing.common.tasklogs.SwitchingTaskLogsFactory;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogKiller;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.apache.druid.tasklogs.TaskLogs;
import org.apache.druid.tasklogs.TaskPayloadManager;

/**
 */
public class IndexingServiceTaskLogsModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoice(binder, "druid.indexer.logs.type", Key.get(TaskLogs.class), Key.get(FileTaskLogs.class));
    PolyBind.createChoice(binder, "druid.indexer.logs.switching.delegateType", Key.get(TaskLogs.class, Names.named("delegate")), Key.get(FileTaskLogs.class));

    JsonConfigProvider.bind(binder, "druid.indexer.logs", FileTaskLogsConfig.class);

    final MapBinder<String, TaskLogs> taskLogBinder = Binders.taskLogsBinder(binder);
    taskLogBinder.addBinding("switching").to(SwitchingTaskLogs.class).in(LazySingleton.class);

    Binders.bindTaskLogs(binder, "noop", NoopTaskLogs.class);
    Binders.bindTaskLogs(binder, "file", FileTaskLogs.class);

    binder.bind(NoopTaskLogs.class).in(LazySingleton.class);
    binder.bind(FileTaskLogs.class).in(LazySingleton.class);
    binder.bind(SwitchingTaskLogs.class).in(LazySingleton.class);
    binder.bind(SwitchingTaskLogsFactory.class).in(LazySingleton.class);

    binder.bind(TaskLogPusher.class).to(TaskLogs.class);
    binder.bind(TaskLogKiller.class).to(TaskLogs.class);
    binder.bind(TaskPayloadManager.class).to(TaskLogs.class);
  }

  @Provides
  @Named("streamer")
  public TaskLogs provideStreamer(SwitchingTaskLogsFactory factory)
  {
    return factory.createStreamer();
  }

  @Provides
  @Named("pusher")
  public TaskLogs providePusher(SwitchingTaskLogsFactory factory)
  {
    return factory.createPusher();
  }
}

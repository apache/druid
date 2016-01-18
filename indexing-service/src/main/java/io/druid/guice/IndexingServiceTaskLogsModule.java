/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.druid.indexing.common.config.FileTaskLogsConfig;
import io.druid.indexing.common.tasklogs.FileTaskLogs;
import io.druid.tasklogs.NoopTaskLogs;
import io.druid.tasklogs.TaskLogPusher;
import io.druid.tasklogs.TaskLogs;

/**
 */
public class IndexingServiceTaskLogsModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoice(binder, "druid.indexer.logs.type", Key.get(TaskLogs.class), Key.get(FileTaskLogs.class));
    JsonConfigProvider.bind(binder, "druid.indexer.logs", FileTaskLogsConfig.class);

    final MapBinder<String, TaskLogs> taskLogBinder = Binders.taskLogsBinder(binder);
    taskLogBinder.addBinding("noop").to(NoopTaskLogs.class).in(LazySingleton.class);
    taskLogBinder.addBinding("file").to(FileTaskLogs.class).in(LazySingleton.class);
    binder.bind(NoopTaskLogs.class).in(LazySingleton.class);
    binder.bind(FileTaskLogs.class).in(LazySingleton.class);

    binder.bind(TaskLogPusher.class).to(TaskLogs.class);
  }
}

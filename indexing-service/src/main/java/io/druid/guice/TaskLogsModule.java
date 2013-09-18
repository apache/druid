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
import com.google.inject.multibindings.MapBinder;
import io.druid.indexing.common.tasklogs.NoopTaskLogs;
import io.druid.indexing.common.tasklogs.S3TaskLogs;
import io.druid.indexing.common.tasklogs.S3TaskLogsConfig;
import io.druid.indexing.common.tasklogs.TaskLogPusher;
import io.druid.indexing.common.tasklogs.TaskLogs;

/**
 */
public class TaskLogsModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoice(binder, "druid.indexer.logs.type", Key.get(TaskLogs.class), Key.get(NoopTaskLogs.class));
    final MapBinder<String, TaskLogs> taskLogBinder = PolyBind.optionBinder(binder, Key.get(TaskLogs.class));

    JsonConfigProvider.bind(binder, "druid.indexer.logs", S3TaskLogsConfig.class);
    taskLogBinder.addBinding("s3").to(S3TaskLogs.class);
    binder.bind(S3TaskLogs.class).in(LazySingleton.class);

    taskLogBinder.addBinding("noop").to(NoopTaskLogs.class).in(LazySingleton.class);
    binder.bind(NoopTaskLogs.class).in(LazySingleton.class);

    binder.bind(TaskLogPusher.class).to(TaskLogs.class);
  }
}

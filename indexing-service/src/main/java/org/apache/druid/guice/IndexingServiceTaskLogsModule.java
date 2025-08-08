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
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import org.apache.druid.indexing.common.config.FileTaskLogsConfig;
import org.apache.druid.indexing.common.tasklogs.FileTaskLogs;
import org.apache.druid.indexing.common.tasklogs.SwitchingTaskLogs;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogKiller;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.apache.druid.tasklogs.TaskLogs;
import org.apache.druid.tasklogs.TaskPayloadManager;

import java.util.Properties;

/**
 *
 */
public class IndexingServiceTaskLogsModule implements Module
{
  private Properties props;

  @Inject
  public IndexingServiceTaskLogsModule(Properties props)
  {
    this.props = props;
  }

  @Override
  public void configure(Binder binder)
  {
      PolyBind.createChoice(binder, "druid.indexer.logs.type", Key.get(TaskLogs.class), Key.get(FileTaskLogs.class));
      PolyBind.createChoice(
          binder,
          SwitchingTaskLogs.PROPERTY_KEY_SWITCHING_DEFAULT_TYPE,
          Key.get(TaskLogs.class, Names.named(SwitchingTaskLogs.KEY_SWITCHING_DEFAULT)),
          Key.get(FileTaskLogs.class)
      );

    if (props != null && props.getProperty(SwitchingTaskLogs.PROPERTY_KEY_SWITCHING_PUSH_TYPE) != null) {
      PolyBind.createChoice(
          binder,
          SwitchingTaskLogs.PROPERTY_KEY_SWITCHING_PUSH_TYPE,
          Key.get(TaskLogs.class, Names.named(SwitchingTaskLogs.KEY_SWITCHING_PUSH)),
          null
      );
    } else {
      binder.bind(Key.get(TaskLogs.class, Names.named(SwitchingTaskLogs.KEY_SWITCHING_PUSH)))
            .toProvider(() -> null);
    }

    if (props != null && props.getProperty(SwitchingTaskLogs.PROPERTY_KEY_SWITCHING_REPORTS_TYPE) != null) {
      PolyBind.createChoice(
          binder,
          SwitchingTaskLogs.PROPERTY_KEY_SWITCHING_REPORTS_TYPE,
          Key.get(TaskLogs.class, Names.named(SwitchingTaskLogs.KEY_SWITCHING_REPORTS)),
          null
      );
    } else {
      binder.bind(Key.get(TaskLogs.class, Names.named(SwitchingTaskLogs.KEY_SWITCHING_REPORTS)))
            .toProvider(() -> null);
    }

    if (props != null && props.getProperty(SwitchingTaskLogs.PROPERTY_KEY_SWITCHING_STREAM_TYPE) != null) {
      PolyBind.createChoice(
          binder,
          SwitchingTaskLogs.PROPERTY_KEY_SWITCHING_STREAM_TYPE,
          Key.get(TaskLogs.class, Names.named(SwitchingTaskLogs.KEY_SWITCHING_STREAM)),
          null
      );
    } else {
      binder.bind(Key.get(TaskLogs.class, Names.named(SwitchingTaskLogs.KEY_SWITCHING_STREAM)))
            .toProvider(() -> null);
    }


    JsonConfigProvider.bind(binder, "druid.indexer.logs", FileTaskLogsConfig.class);

    final MapBinder<String, TaskLogs> taskLogBinder = Binders.taskLogsBinder(binder);
    taskLogBinder.addBinding("switching").to(SwitchingTaskLogs.class);

    Binders.bindTaskLogs(binder, "noop", NoopTaskLogs.class);
    Binders.bindTaskLogs(binder, "file", FileTaskLogs.class);

    binder.bind(NoopTaskLogs.class).in(LazySingleton.class);
    binder.bind(FileTaskLogs.class).in(LazySingleton.class);
    binder.bind(SwitchingTaskLogs.class).in(LazySingleton.class);

    binder.bind(TaskLogPusher.class).to(TaskLogs.class);
    binder.bind(TaskLogKiller.class).to(TaskLogs.class);
    binder.bind(TaskPayloadManager.class).to(TaskLogs.class);
  }
}

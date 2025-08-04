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
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.druid.indexing.common.config.FileTaskLogsConfig;
import org.apache.druid.indexing.common.tasklogs.FileTaskLogs;
import org.apache.druid.indexing.common.tasklogs.SwitchingTaskLogs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
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
  private static final Logger log = new EmittingLogger(IndexingServiceTaskLogsModule.class);
  private static final String PROPERTY_PREFIX_SWITCHING = "druid.indexer.logs.switching";
  private static final String PROPERTY_KEY_SWITCHING_PUSH_TYPE = PROPERTY_PREFIX_SWITCHING + ".pushType";
  private static final String PROPERTY_KEY_SWITCHING_STREAM_TYPE = PROPERTY_PREFIX_SWITCHING + ".streamType";
  private static final String PROPERTY_KEY_SWITCHING_REPORTS_TYPE = PROPERTY_PREFIX_SWITCHING + ".reportsType";

  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoice(binder, "druid.indexer.logs.type", Key.get(TaskLogs.class), Key.get(FileTaskLogs.class));
    PolyBind.createChoice(
        binder,
        PROPERTY_PREFIX_SWITCHING + ".defaultType",
        Key.get(TaskLogs.class, Names.named("defaultType")),
        Key.get(FileTaskLogs.class)
    );


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

  @Provides
  @Named("streamer")
  public TaskLogs provideStreamer(
      Properties properties,
      Injector injector,
      @Named("defaultType") TaskLogs defaultTaskLogs
  )
  {
    String logStreamerType = properties.getProperty(PROPERTY_KEY_SWITCHING_STREAM_TYPE);
    if (logStreamerType != null) {
      try {
        return injector.getInstance(Key.get(TaskLogs.class, Names.named(logStreamerType)));
      }
      catch (Exception e) {
        log.warn(e, "Failed to get TaskLogs for type[%s], using default", logStreamerType);
      }
    }
    return defaultTaskLogs;
  }

  @Provides
  @Named("pusher")
  public TaskLogs providePusher(
      Properties properties,
      Injector injector,
      @Named("defaultType") TaskLogs defaultTaskLogs
  )
  {
    String logPusherType = properties.getProperty(PROPERTY_KEY_SWITCHING_PUSH_TYPE);
    if (logPusherType != null) {
      try {
        return injector.getInstance(Key.get(TaskLogs.class, Names.named(logPusherType)));
      }
      catch (Exception e) {
        log.warn(e, "Failed to get TaskLogs for type[%s], using default", logPusherType);
      }
    }
    return defaultTaskLogs;
  }

  @Provides
  @Named("reports")
  public TaskLogs provideDelegate(
      Properties properties,
      Injector injector,
      @Named("defaultType") TaskLogs defaultTaskLogs
  )
  {
    String reportsType = properties.getProperty(PROPERTY_KEY_SWITCHING_REPORTS_TYPE);
    if (reportsType != null) {
      try {
        return injector.getInstance(Key.get(TaskLogs.class, Names.named(reportsType)));
      }
      catch (Exception e) {
        log.warn(e, "Failed to get TaskLogs for type[%s], using default", reportsType);
      }
    }
    return defaultTaskLogs;
  }
}

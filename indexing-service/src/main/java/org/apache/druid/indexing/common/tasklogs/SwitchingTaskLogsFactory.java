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

package org.apache.druid.indexing.common.tasklogs;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.tasklogs.TaskLogs;

import java.util.Properties;

public class SwitchingTaskLogsFactory
{
  private static final Logger log = new Logger(SwitchingTaskLogsFactory.class);
  private static final String SWITCHING_TYPE = "switching";

  private final Properties properties;
  private final Injector injector;

  @Inject
  public SwitchingTaskLogsFactory(
      Properties properties,
      @Self Injector injector
  )
  {
    this.properties = properties;
    this.injector = injector;
  }

  public TaskLogs createStreamer()
  {
    String logsType = properties.getProperty("druid.indexer.logs.switching.logsType", "file");

    if (SWITCHING_TYPE.equals(logsType)) {
      String streamType = properties.getProperty("druid.indexer.logs.switching.logsType.logsStreamType", "file");
      log.info("Recursive switching detected for streamer, using streamType: %s", streamType);
      return getTaskLogsFromInjector(streamType);
    }

    return getTaskLogsFromInjector(logsType);
  }

  public TaskLogs createPusher()
  {
    String logsType = properties.getProperty("druid.indexer.logs.switching.logsType", "file");

    if (SWITCHING_TYPE.equals(logsType)) {
      String pushType = properties.getProperty("druid.indexer.logs.switching.logsType.logsPushType", "file");
      log.info("Recursive switching detected for pusher, using pushType: %s", pushType);
      return getTaskLogsFromInjector(pushType);
    }

    return getTaskLogsFromInjector(logsType);
  }

  private TaskLogs getTaskLogsFromInjector(String type)
  {
    try {
      Key<TaskLogs> key = Key.get(TaskLogs.class, Names.named(type));
      return injector.getInstance(key);
    }
    catch (Exception e) {
      log.warn("No TaskLogs binding found for type: %s, falling back to 'file'. Error: %s", type, e.getMessage());
      try {
        Key<TaskLogs> fileKey = Key.get(TaskLogs.class, Names.named("file"));
        return injector.getInstance(fileKey);
      }
      catch (Exception fallbackException) {
        throw new IllegalStateException("No TaskLogs binding found for fallback type 'file'", fallbackException);
      }
    }
  }
}
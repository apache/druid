/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.AbstractTask;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@JsonTypeName("busyTask")
public class BusyTask extends AbstractTask
{
  private static final Logger log = new Logger(BusyTask.class);
  private final String lockFile;
  private final long sleep;

  public BusyTask(
      @JsonProperty("id") String id,
      @JsonProperty("lockFile") String lockFile,
      @JsonProperty("sleep") long sleep
  )
  {
    super(
        id == null ? "testTask-" + UUID.randomUUID().toString() : id,
        "noDataSource",
        ImmutableMap.<String, Object>of()
    );
    this.lockFile = lockFile;
    this.sleep = sleep;
  }

  @JsonProperty("lockFile")
  public String getLockFile()
  {
    return lockFile;
  }

  @JsonProperty("sleep")
  public long getSleep()
  {
    return sleep;
  }

  @Override
  public String getType()
  {
    return "busyTask";
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    log.info("Deleting file at [%s]", getLockFile());
    File file = new File(getLockFile());
    if (!file.createNewFile()) {
      log.error("Error deleting file at [%s]", file);
    }
    final Path path = file.toPath();
    while (file.exists()) {
      try (WatchService service = path.getFileSystem().newWatchService()) {
        path.getParent().register(service, StandardWatchEventKinds.ENTRY_DELETE);
        if (file.exists()) {
          WatchKey key = service.poll(sleep, TimeUnit.MILLISECONDS);
          if (key == null) {
            log.error("Ran out of time waiting for [%s]", path);
            return TaskStatus.failure(getId());
          }
          log.info("Delete event found for [%s]", path);
        }
      }
    }
    return TaskStatus.success(getId());
  }
}

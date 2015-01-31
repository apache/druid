/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.MonitorScheduler;
import io.druid.client.FilteredServerView;
import io.druid.guice.annotations.Processing;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.Task;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.loading.DataSegmentArchiver;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.server.coordination.DataSegmentAnnouncer;

import java.io.File;
import java.util.concurrent.ExecutorService;

/**
 * Stuff that may be needed by a Task in order to conduct its business.
 */
public class TaskToolboxFactory
{
  private final TaskConfig config;
  private final TaskActionClientFactory taskActionClientFactory;
  private final ServiceEmitter emitter;
  private final DataSegmentPusher segmentPusher;
  private final DataSegmentKiller dataSegmentKiller;
  private final DataSegmentMover dataSegmentMover;
  private final DataSegmentArchiver dataSegmentArchiver;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final FilteredServerView newSegmentServerView;
  private final QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate;
  private final ExecutorService queryExecutorService;
  private final MonitorScheduler monitorScheduler;
  private final SegmentLoaderFactory segmentLoaderFactory;
  private final ObjectMapper objectMapper;

  @Inject
  public TaskToolboxFactory(
      TaskConfig config,
      TaskActionClientFactory taskActionClientFactory,
      ServiceEmitter emitter,
      DataSegmentPusher segmentPusher,
      DataSegmentKiller dataSegmentKiller,
      DataSegmentMover dataSegmentMover,
      DataSegmentArchiver dataSegmentArchiver,
      DataSegmentAnnouncer segmentAnnouncer,
      FilteredServerView newSegmentServerView,
      QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate,
      @Processing ExecutorService queryExecutorService,
      MonitorScheduler monitorScheduler,
      SegmentLoaderFactory segmentLoaderFactory,
      ObjectMapper objectMapper
  )
  {
    this.config = config;
    this.taskActionClientFactory = taskActionClientFactory;
    this.emitter = emitter;
    this.segmentPusher = segmentPusher;
    this.dataSegmentKiller = dataSegmentKiller;
    this.dataSegmentMover = dataSegmentMover;
    this.dataSegmentArchiver = dataSegmentArchiver;
    this.segmentAnnouncer = segmentAnnouncer;
    this.newSegmentServerView = newSegmentServerView;
    this.queryRunnerFactoryConglomerate = queryRunnerFactoryConglomerate;
    this.queryExecutorService = queryExecutorService;
    this.monitorScheduler = monitorScheduler;
    this.segmentLoaderFactory = segmentLoaderFactory;
    this.objectMapper = objectMapper;
  }

  public TaskToolbox build(Task task)
  {
    final File taskWorkDir = new File(new File(config.getBaseTaskDir(), task.getId()), "work");

    return new TaskToolbox(
        config,
        task,
        taskActionClientFactory,
        emitter,
        segmentPusher,
        dataSegmentKiller,
        dataSegmentMover,
        dataSegmentArchiver,
        segmentAnnouncer,
        newSegmentServerView,
        queryRunnerFactoryConglomerate,
        queryExecutorService,
        monitorScheduler,
        segmentLoaderFactory.manufacturate(taskWorkDir),
        objectMapper,
        taskWorkDir
    );
  }
}

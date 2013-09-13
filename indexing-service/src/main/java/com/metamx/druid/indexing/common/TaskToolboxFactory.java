/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.druid.client.ServerView;
import com.metamx.druid.coordination.DataSegmentAnnouncer;
import com.metamx.druid.loading.DataSegmentKiller;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.indexing.common.actions.TaskActionClientFactory;
import com.metamx.druid.indexing.common.config.TaskConfig;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.MonitorScheduler;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;

import java.util.concurrent.ExecutorService;

/**
 * Stuff that may be needed by a Task in order to conduct its business.
 */
public class TaskToolboxFactory
{
  private final TaskConfig config;
  private final TaskActionClientFactory taskActionClientFactory;
  private final ServiceEmitter emitter;
  private final RestS3Service s3Client;
  private final DataSegmentPusher segmentPusher;
  private final DataSegmentKiller dataSegmentKiller;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final ServerView newSegmentServerView;
  private final QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate;
  private final ExecutorService queryExecutorService;
  private final MonitorScheduler monitorScheduler;
  private final ObjectMapper objectMapper;

  public TaskToolboxFactory(
      TaskConfig config,
      TaskActionClientFactory taskActionClientFactory,
      ServiceEmitter emitter,
      RestS3Service s3Client,
      DataSegmentPusher segmentPusher,
      DataSegmentKiller dataSegmentKiller,
      DataSegmentAnnouncer segmentAnnouncer,
      ServerView newSegmentServerView,
      QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate,
      ExecutorService queryExecutorService,
      MonitorScheduler monitorScheduler,
      ObjectMapper objectMapper
  )
  {
    this.config = config;
    this.taskActionClientFactory = taskActionClientFactory;
    this.emitter = emitter;
    this.s3Client = s3Client;
    this.segmentPusher = segmentPusher;
    this.dataSegmentKiller = dataSegmentKiller;
    this.segmentAnnouncer = segmentAnnouncer;
    this.newSegmentServerView = newSegmentServerView;
    this.queryRunnerFactoryConglomerate = queryRunnerFactoryConglomerate;
    this.queryExecutorService = queryExecutorService;
    this.monitorScheduler = monitorScheduler;
    this.objectMapper = objectMapper;
  }

  public TaskToolbox build(Task task)
  {
    return new TaskToolbox(
        config,
        task,
        taskActionClientFactory,
        emitter,
        s3Client,
        segmentPusher,
        dataSegmentKiller,
        segmentAnnouncer,
        newSegmentServerView,
        queryRunnerFactoryConglomerate,
        queryExecutorService,
        monitorScheduler,
        objectMapper
    );
  }
}

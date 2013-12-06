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

package io.druid.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.MonitorScheduler;
import io.druid.client.ServerView;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.Task;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Stuff that may be needed by a Task in order to conduct its business.
 */
public class TaskToolbox
{
  private final TaskConfig config;
  private final Task task;
  private final TaskActionClientFactory taskActionClientFactory;
  private final ServiceEmitter emitter;
  private final DataSegmentPusher segmentPusher;
  private final DataSegmentKiller dataSegmentKiller;
  private final DataSegmentMover dataSegmentMover;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final ServerView newSegmentServerView;
  private final QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate;
  private final MonitorScheduler monitorScheduler;
  private final ExecutorService queryExecutorService;
  private final SegmentLoader segmentLoader;
  private final ObjectMapper objectMapper;
  private final File taskWorkDir;

  public TaskToolbox(
      TaskConfig config,
      Task task,
      TaskActionClientFactory taskActionClientFactory,
      ServiceEmitter emitter,
      DataSegmentPusher segmentPusher,
      DataSegmentKiller dataSegmentKiller,
      DataSegmentMover dataSegmentMover,
      DataSegmentAnnouncer segmentAnnouncer,
      ServerView newSegmentServerView,
      QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate,
      ExecutorService queryExecutorService,
      MonitorScheduler monitorScheduler,
      SegmentLoader segmentLoader,
      ObjectMapper objectMapper,
      final File taskWorkDir
  )
  {
    this.config = config;
    this.task = task;
    this.taskActionClientFactory = taskActionClientFactory;
    this.emitter = emitter;
    this.segmentPusher = segmentPusher;
    this.dataSegmentKiller = dataSegmentKiller;
    this.dataSegmentMover = dataSegmentMover;
    this.segmentAnnouncer = segmentAnnouncer;
    this.newSegmentServerView = newSegmentServerView;
    this.queryRunnerFactoryConglomerate = queryRunnerFactoryConglomerate;
    this.queryExecutorService = queryExecutorService;
    this.monitorScheduler = monitorScheduler;
    this.segmentLoader = segmentLoader;
    this.objectMapper = objectMapper;
    this.taskWorkDir = taskWorkDir;
  }

  public TaskConfig getConfig()
  {
    return config;
  }

  public TaskActionClient getTaskActionClient()
  {
    return taskActionClientFactory.create(task);
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public DataSegmentPusher getSegmentPusher()
  {
    return segmentPusher;
  }

  public DataSegmentKiller getDataSegmentKiller()
  {
    return dataSegmentKiller;
  }

  public DataSegmentMover getDataSegmentMover()
  {
    return dataSegmentMover;
  }

  public DataSegmentAnnouncer getSegmentAnnouncer()
  {
    return segmentAnnouncer;
  }

  public ServerView getNewSegmentServerView()
  {
    return newSegmentServerView;
  }

  public QueryRunnerFactoryConglomerate getQueryRunnerFactoryConglomerate()
  {
    return queryRunnerFactoryConglomerate;
  }

  public ExecutorService getQueryExecutorService()
  {
    return queryExecutorService;
  }

  public MonitorScheduler getMonitorScheduler()
  {
    return monitorScheduler;
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public Map<DataSegment, File> getSegments(List<DataSegment> segments)
      throws SegmentLoadingException
  {
    Map<DataSegment, File> retVal = Maps.newLinkedHashMap();
    for (DataSegment segment : segments) {
      retVal.put(segment, segmentLoader.getSegmentFiles(segment));
    }

    return retVal;
  }

  public File getTaskWorkDir()
  {
    return taskWorkDir;
  }
}

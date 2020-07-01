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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.inject.Provider;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.indexing.common.actions.SegmentInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.worker.IntermediaryDataManager;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.Monitor;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Stuff that may be needed by a Task in order to conduct its business.
 */
public class TaskToolbox
{
  private final TaskConfig config;
  private final DruidNode taskExecutorNode;
  private final TaskActionClient taskActionClient;
  private final ServiceEmitter emitter;
  private final DataSegmentPusher segmentPusher;
  private final DataSegmentKiller dataSegmentKiller;
  private final DataSegmentArchiver dataSegmentArchiver;
  private final DataSegmentMover dataSegmentMover;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final DataSegmentServerAnnouncer serverAnnouncer;
  private final SegmentHandoffNotifierFactory handoffNotifierFactory;
  /**
   * Using Provider, not {@link QueryRunnerFactoryConglomerate} directly, to not require {@link
   * org.apache.druid.indexing.overlord.TaskRunner} implementations that create TaskToolboxes to inject query stuff eagerly,
   * because it may be unavailable, e. g. for batch tasks running in Spark or Hadoop.
   */
  private final Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider;
  @Nullable
  private final Provider<MonitorScheduler> monitorSchedulerProvider;
  private final ExecutorService queryExecutorService;
  private final JoinableFactory joinableFactory;
  private final SegmentLoader segmentLoader;
  private final ObjectMapper jsonMapper;
  private final File taskWorkDir;
  private final IndexIO indexIO;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;
  private final IndexMergerV9 indexMergerV9;
  private final TaskReportFileWriter taskReportFileWriter;

  private final DruidNodeAnnouncer druidNodeAnnouncer;
  private final DruidNode druidNode;
  private final LookupNodeService lookupNodeService;
  private final DataNodeService dataNodeService;
  private final IntermediaryDataManager intermediaryDataManager;

  public TaskToolbox(
      TaskConfig config,
      DruidNode taskExecutorNode,
      TaskActionClient taskActionClient,
      ServiceEmitter emitter,
      DataSegmentPusher segmentPusher,
      DataSegmentKiller dataSegmentKiller,
      DataSegmentMover dataSegmentMover,
      DataSegmentArchiver dataSegmentArchiver,
      DataSegmentAnnouncer segmentAnnouncer,
      DataSegmentServerAnnouncer serverAnnouncer,
      SegmentHandoffNotifierFactory handoffNotifierFactory,
      Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider,
      ExecutorService queryExecutorService,
      JoinableFactory joinableFactory,
      @Nullable Provider<MonitorScheduler> monitorSchedulerProvider,
      SegmentLoader segmentLoader,
      ObjectMapper jsonMapper,
      File taskWorkDir,
      IndexIO indexIO,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats,
      IndexMergerV9 indexMergerV9,
      DruidNodeAnnouncer druidNodeAnnouncer,
      DruidNode druidNode,
      LookupNodeService lookupNodeService,
      DataNodeService dataNodeService,
      TaskReportFileWriter taskReportFileWriter,
      IntermediaryDataManager intermediaryDataManager
  )
  {
    this.config = config;
    this.taskExecutorNode = taskExecutorNode;
    this.taskActionClient = taskActionClient;
    this.emitter = emitter;
    this.segmentPusher = segmentPusher;
    this.dataSegmentKiller = dataSegmentKiller;
    this.dataSegmentMover = dataSegmentMover;
    this.dataSegmentArchiver = dataSegmentArchiver;
    this.segmentAnnouncer = segmentAnnouncer;
    this.serverAnnouncer = serverAnnouncer;
    this.handoffNotifierFactory = handoffNotifierFactory;
    this.queryRunnerFactoryConglomerateProvider = queryRunnerFactoryConglomerateProvider;
    this.queryExecutorService = queryExecutorService;
    this.joinableFactory = joinableFactory;
    this.monitorSchedulerProvider = monitorSchedulerProvider;
    this.segmentLoader = segmentLoader;
    this.jsonMapper = jsonMapper;
    this.taskWorkDir = taskWorkDir;
    this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.cachePopulatorStats = cachePopulatorStats;
    this.indexMergerV9 = Preconditions.checkNotNull(indexMergerV9, "Null IndexMergerV9");
    this.druidNodeAnnouncer = druidNodeAnnouncer;
    this.druidNode = druidNode;
    this.lookupNodeService = lookupNodeService;
    this.dataNodeService = dataNodeService;
    this.taskReportFileWriter = taskReportFileWriter;
    this.taskReportFileWriter.setObjectMapper(this.jsonMapper);
    this.intermediaryDataManager = intermediaryDataManager;
  }

  public TaskConfig getConfig()
  {
    return config;
  }

  public DruidNode getTaskExecutorNode()
  {
    return taskExecutorNode;
  }

  public TaskActionClient getTaskActionClient()
  {
    return taskActionClient;
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

  public DataSegmentArchiver getDataSegmentArchiver()
  {
    return dataSegmentArchiver;
  }

  public DataSegmentAnnouncer getSegmentAnnouncer()
  {
    return segmentAnnouncer;
  }

  public DataSegmentServerAnnouncer getDataSegmentServerAnnouncer()
  {
    return serverAnnouncer;
  }

  public SegmentHandoffNotifierFactory getSegmentHandoffNotifierFactory()
  {
    return handoffNotifierFactory;
  }

  public QueryRunnerFactoryConglomerate getQueryRunnerFactoryConglomerate()
  {
    return queryRunnerFactoryConglomerateProvider.get();
  }

  public ExecutorService getQueryExecutorService()
  {
    return queryExecutorService;
  }

  public JoinableFactory getJoinableFactory()
  {
    return joinableFactory;
  }

  @Nullable
  public MonitorScheduler getMonitorScheduler()
  {
    return monitorSchedulerProvider == null ? null : monitorSchedulerProvider.get();
  }

  /**
   * Adds a monitor to the monitorScheduler if it is configured
   * @param monitor
   */
  public void addMonitor(Monitor monitor)
  {
    MonitorScheduler scheduler = getMonitorScheduler();
    if (scheduler != null) {
      scheduler.addMonitor(monitor);
    }
  }

  /**
   * Adds a monitor to the monitorScheduler if it is configured
   * @param monitor
   */
  public void removeMonitor(Monitor monitor)
  {
    MonitorScheduler scheduler = getMonitorScheduler();
    if (scheduler != null) {
      scheduler.removeMonitor(monitor);
    }
  }

  public ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  public Map<DataSegment, File> fetchSegments(List<DataSegment> segments)
      throws SegmentLoadingException
  {
    Map<DataSegment, File> retVal = Maps.newLinkedHashMap();
    for (DataSegment segment : segments) {
      retVal.put(segment, segmentLoader.getSegmentFiles(segment));
    }

    return retVal;
  }

  public void publishSegments(Iterable<DataSegment> segments) throws IOException
  {
    // Request segment pushes for each set
    final Multimap<Interval, DataSegment> segmentMultimap = Multimaps.index(
        segments,
        new Function<DataSegment, Interval>()
        {
          @Override
          public Interval apply(DataSegment segment)
          {
            return segment.getInterval();
          }
        }
    );
    for (final Collection<DataSegment> segmentCollection : segmentMultimap.asMap().values()) {
      getTaskActionClient().submit(new SegmentInsertAction(ImmutableSet.copyOf(segmentCollection)));
    }
  }

  public IndexIO getIndexIO()
  {
    return indexIO;
  }

  public Cache getCache()
  {
    return cache;
  }

  public CacheConfig getCacheConfig()
  {
    return cacheConfig;
  }

  public CachePopulatorStats getCachePopulatorStats()
  {
    return cachePopulatorStats;
  }

  public IndexMergerV9 getIndexMergerV9()
  {
    return indexMergerV9;
  }

  public File getIndexingTmpDir()
  {
    final File tmpDir = new File(taskWorkDir, "indexing-tmp");
    try {
      FileUtils.forceMkdir(tmpDir);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return tmpDir;
  }

  public File getMergeDir()
  {
    return new File(taskWorkDir, "merge");
  }

  public File getPersistDir()
  {
    return new File(taskWorkDir, "persist");
  }

  public DruidNodeAnnouncer getDruidNodeAnnouncer()
  {
    return druidNodeAnnouncer;
  }

  public LookupNodeService getLookupNodeService()
  {
    return lookupNodeService;
  }

  public DataNodeService getDataNodeService()
  {
    return dataNodeService;
  }

  public DruidNode getDruidNode()
  {
    return druidNode;
  }

  public TaskReportFileWriter getTaskReportFileWriter()
  {
    return taskReportFileWriter;
  }

  public IntermediaryDataManager getIntermediaryDataManager()
  {
    return intermediaryDataManager;
  }
}

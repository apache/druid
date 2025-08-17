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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.catalog.MapMetadataCatalog;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.IndexingTemplateDefn;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.ClientMSQContext;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.SupervisorModule;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatistics;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.compaction.Table;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class OverlordCompactionSchedulerTest
{
  private static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new DefaultObjectMapper();
    OBJECT_MAPPER.registerModules(new IndexingServiceTuningConfigModule().getJacksonModules());
    OBJECT_MAPPER.registerModules(new SupervisorModule().getJacksonModules());
    OBJECT_MAPPER.setInjectableValues(
        new InjectableValues
            .Std()
            .addValue(
                SegmentCacheManagerFactory.class,
                new SegmentCacheManagerFactory(TestIndex.INDEX_IO, OBJECT_MAPPER)
            )
    );
  }

  private static final DateTime JAN_20 = DateTimes.of("2025-01-20");
  private static final DateTime MAR_11 = DateTimes.of("2025-03-11");

  private AtomicReference<ClusterCompactionConfig> compactionConfig;
  private CoordinatorOverlordServiceConfig coordinatorOverlordServiceConfig;

  private TaskMaster taskMaster;
  private TaskQueue taskQueue;
  private BrokerClient brokerClient;
  private TaskActionClientFactory taskActionClientFactory;
  private BlockingExecutorService executor;

  private HeapMemoryTaskStorage taskStorage;
  private TestIndexerMetadataStorageCoordinator segmentStorage;
  private SegmentsMetadataManager segmentsMetadataManager;
  private StubServiceEmitter serviceEmitter;

  private String dataSource;
  private MapMetadataCatalog catalog;
  private OverlordCompactionScheduler scheduler;

  private Map<Interval, String> submittedMsqTaskIds;

  @Before
  public void setUp()
  {
    dataSource = "wiki_" + IdUtils.getRandomId();

    final TaskRunner taskRunner = Mockito.mock(TaskRunner.class);
    Mockito.when(taskRunner.getTotalCapacity()).thenReturn(100);
    Mockito.when(taskRunner.getMaximumCapacityWithAutoscale()).thenReturn(100);

    taskQueue = Mockito.mock(TaskQueue.class);
    catalog = new MapMetadataCatalog(OBJECT_MAPPER);

    submittedMsqTaskIds = new HashMap<>();
    brokerClient = Mockito.mock(BrokerClient.class);
    Mockito.when(brokerClient.submitSqlTask(ArgumentMatchers.any(ClientSqlQuery.class))).thenAnswer(
        arg -> {
          final ClientSqlQuery query = arg.getArgument(0);
          final Interval compactionInterval =
              (Interval) query.getContext().get(CompactSegments.COMPACTION_INTERVAL_KEY);

          final String taskId = IdUtils.getRandomId();
          submittedMsqTaskIds.put(compactionInterval, taskId);
          return Futures.immediateFuture(new SqlTaskStatus(taskId, TaskState.RUNNING, null));
        }
    );

    taskMaster = new TaskMaster(null, null);
    Assert.assertFalse(taskMaster.isHalfOrFullLeader());
    Assert.assertFalse(taskMaster.isFullLeader());

    taskMaster.becomeHalfLeader(taskRunner, taskQueue);
    Assert.assertTrue(taskMaster.isHalfOrFullLeader());
    Assert.assertFalse(taskMaster.isFullLeader());

    taskMaster.becomeFullLeader();
    Assert.assertTrue(taskMaster.isHalfOrFullLeader());
    Assert.assertTrue(taskMaster.isFullLeader());

    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));

    executor = new BlockingExecutorService("test");
    serviceEmitter = new StubServiceEmitter();
    segmentStorage = new TestIndexerMetadataStorageCoordinator();
    segmentsMetadataManager = segmentStorage.getManager();

    compactionConfig = new AtomicReference<>(new ClusterCompactionConfig(1.0, 100, null, true, null));
    coordinatorOverlordServiceConfig = new CoordinatorOverlordServiceConfig(false, null);

    taskActionClientFactory = task -> new TaskActionClient()
    {
      @Override
      @SuppressWarnings("unchecked")
      public <RetType> RetType submit(TaskAction<RetType> taskAction)
      {
        if (taskAction instanceof RetrieveUsedSegmentsAction) {
          return (RetType) segmentStorage.retrieveAllUsedSegments(
              ((RetrieveUsedSegmentsAction) taskAction).getDataSource(),
              Segments.ONLY_VISIBLE
          );
        } else if (taskAction instanceof TimeChunkLockTryAcquireAction) {
          final TimeChunkLockTryAcquireAction lockAcquireAction = (TimeChunkLockTryAcquireAction) taskAction;
          return (RetType) new TimeChunkLock(
              null,
              task.getGroupId(),
              task.getDataSource(),
              lockAcquireAction.getInterval(),
              DateTimes.nowUtc().toString(),
              1
          );
        } else {
          return null;
        }
      }
    };

    initScheduler();
  }

  private void initScheduler()
  {
    GlobalTaskLockbox taskLockbox = new GlobalTaskLockbox(taskStorage, new TestIndexerMetadataStorageCoordinator());
    taskLockbox.syncFromStorage();
    WorkerBehaviorConfig defaultWorkerConfig
        = new DefaultWorkerBehaviorConfig(WorkerBehaviorConfig.DEFAULT_STRATEGY, null);
    scheduler = new OverlordCompactionScheduler(
        taskMaster,
        taskLockbox,
        new TaskQueryTool(taskStorage, taskLockbox, taskMaster, null, () -> defaultWorkerConfig),
        segmentsMetadataManager,
        new SegmentsMetadataManagerConfig(null, null, null),
        () -> DruidCompactionConfig.empty().withClusterConfig(compactionConfig.get()),
        new CompactionStatusTracker(),
        coordinatorOverlordServiceConfig,
        taskActionClientFactory,
        new DruidInputSourceFactory(
            TestIndex.INDEX_IO,
            Mockito.mock(TaskConfig.class),
            new NoopCoordinatorClient(),
            new SegmentCacheManagerFactory(TestIndex.INDEX_IO, OBJECT_MAPPER)
        ),
        (nameFormat, numThreads) -> new WrappingScheduledExecutorService("test", executor, false),
        brokerClient,
        serviceEmitter,
        OBJECT_MAPPER
    );
  }

  @Test
  public void test_becomeLeader_triggersStart_ifEnabled()
  {
    Assert.assertTrue(scheduler.isEnabled());

    Assert.assertFalse(scheduler.isRunning());
    Assert.assertFalse(executor.hasPendingTasks());

    scheduler.becomeLeader();
    runScheduledJob();

    Assert.assertTrue(scheduler.isRunning());
  }

  @Test
  public void test_becomeLeader_doesNotTriggerStart_ifDisabled()
  {
    disableScheduler();
    Assert.assertFalse(scheduler.isEnabled());

    Assert.assertFalse(scheduler.isRunning());

    scheduler.becomeLeader();
    runScheduledJob();

    Assert.assertFalse(scheduler.isRunning());
  }

  @Test
  public void test_stopBeingLeader_triggersStop()
  {
    Assert.assertFalse(scheduler.isRunning());

    scheduler.becomeLeader();
    runScheduledJob();
    Assert.assertTrue(scheduler.isRunning());

    scheduler.stopBeingLeader();
    Assert.assertTrue(scheduler.isRunning());

    runScheduledJob();
    Assert.assertFalse(scheduler.isRunning());
  }

  @Test
  public void test_disableSupervisors_triggersStop()
  {
    // Start scheduler
    scheduler.becomeLeader();
    runScheduledJob();
    Assert.assertTrue(scheduler.isRunning());

    // Disable scheduler to trigger stop
    disableScheduler();
    Assert.assertFalse(scheduler.isEnabled());
    Assert.assertTrue(scheduler.isRunning());

    // Scheduler finally stops in the next schedule cycle
    runScheduledJob();
    Assert.assertFalse(scheduler.isRunning());
  }

  @Test
  public void test_enableSupervisors_triggersStart()
  {
    disableScheduler();

    // Becoming leader does not trigger start since scheduler is disabled
    scheduler.becomeLeader();
    runScheduledJob();
    Assert.assertFalse(scheduler.isRunning());

    // Enable the schduler to trigger start
    enableScheduler();
    Assert.assertFalse(scheduler.isRunning());

    // Scheduler finally starts in the next schedule cycle
    runScheduledJob();
    Assert.assertTrue(scheduler.isRunning());
  }

  @Test
  public void test_disableSupervisors_disablesSegmentPolling()
  {
    disableScheduler();

    verifySegmentPolling(false);
  }

  @Test
  public void test_enableSupervisors_inStandaloneMode_enablesSegmentPolling()
  {
    coordinatorOverlordServiceConfig = new CoordinatorOverlordServiceConfig(false, null);
    initScheduler();

    verifySegmentPolling(true);
  }

  @Test
  public void test_enableSupervisors_inCoordinatorMode_disablesSegmentPolling()
  {
    coordinatorOverlordServiceConfig = new CoordinatorOverlordServiceConfig(true, "overlord");
    initScheduler();

    verifySegmentPolling(false);
  }

  private void verifySegmentPolling(boolean enabled)
  {
    scheduler.becomeLeader();
    runScheduledJob();
    Assert.assertEquals(enabled, segmentsMetadataManager.isPollingDatabasePeriodically());

    scheduler.stopBeingLeader();
    runScheduledJob();
    Assert.assertFalse(segmentsMetadataManager.isPollingDatabasePeriodically());
  }

  @Test
  public void test_validateCompactionConfig_returnsInvalid_forNullConfig()
  {
    final CompactionConfigValidationResult result = scheduler.validateCompactionConfig(null);
    Assert.assertFalse(result.isValid());
    Assert.assertEquals("Cannot be null", result.getReason());
  }

  @Test
  public void test_validateCompactionConfig_returnsInvalid_forMSQConfigWithOneMaxTasks()
  {
    final DataSourceCompactionConfig datasourceConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(dataSource)
        .withEngine(CompactionEngine.MSQ)
        .withTaskContext(Collections.singletonMap(ClientMSQContext.CTX_MAX_NUM_TASKS, 1))
        .build();

    final CompactionConfigValidationResult result = scheduler.validateCompactionConfig(datasourceConfig);
    Assert.assertFalse(result.isValid());
    Assert.assertEquals(
        "MSQ: Context maxNumTasks[1] must be at least 2 (1 controller + 1 worker)",
        result.getReason()
    );
  }

  @Test
  public void test_startCompaction_enablesTaskSubmission_forDatasource()
  {
    createSegments(1, Granularities.DAY, JAN_20);

    scheduler.becomeLeader();
    scheduler.startCompaction(dataSource, createSupervisorWithInlineSpec());

    runCompactionTasks(1);

    final AutoCompactionSnapshot.Builder expectedSnapshot = AutoCompactionSnapshot.builder(dataSource);
    expectedSnapshot.incrementCompactedStats(CompactionStatistics.create(100_000_000, 1, 1));

    Assert.assertEquals(
        expectedSnapshot.build(),
        scheduler.getCompactionSnapshot(dataSource)
    );
    Assert.assertEquals(
        Map.of(dataSource, expectedSnapshot.build()),
        scheduler.getAllCompactionSnapshots()
    );

    serviceEmitter.verifyValue(Stats.Compaction.SUBMITTED_TASKS.getMetricName(), 1L);
    serviceEmitter.verifyValue(Stats.Compaction.COMPACTED_BYTES.getMetricName(), 100_000_000L);

    scheduler.stopBeingLeader();
  }

  @Test
  public void test_stopCompaction_disablesTaskSubmission_forDatasource()
  {
    createSegments(1, Granularities.DAY, JAN_20);

    scheduler.becomeLeader();
    scheduler.startCompaction(dataSource, createSupervisorWithInlineSpec());
    scheduler.stopCompaction(dataSource);

    runScheduledJob();
    Mockito.verify(taskQueue, Mockito.never()).add(ArgumentMatchers.any());

    Assert.assertEquals(
        AutoCompactionSnapshot.builder(dataSource)
                              .withStatus(AutoCompactionSnapshot.ScheduleStatus.NOT_ENABLED)
                              .build(),
        scheduler.getCompactionSnapshot(dataSource)
    );
    Assert.assertTrue(scheduler.getAllCompactionSnapshots().isEmpty());

    serviceEmitter.verifyNotEmitted(Stats.Compaction.SUBMITTED_TASKS.getMetricName());
    serviceEmitter.verifyNotEmitted(Stats.Compaction.COMPACTED_BYTES.getMetricName());

    scheduler.stopBeingLeader();
  }

  @Test
  public void test_simulateRunWithConfigUpdate()
  {
    createSegments(1, Granularities.DAY, DateTimes.of("2013-01-01"));

    scheduler.becomeLeader();
    runScheduledJob();

    scheduler.startCompaction(dataSource, createSupervisorWithInlineSpec());

    final CompactionSimulateResult simulateResult = scheduler.simulateRunWithConfigUpdate(
        new ClusterCompactionConfig(null, null, null, null, null)
    );
    Assert.assertEquals(1, simulateResult.getCompactionStates().size());
    final Table pendingCompactionTable = simulateResult.getCompactionStates().get(CompactionStatus.State.PENDING);
    Assert.assertEquals(
        Arrays.asList("dataSource", "interval", "numSegments", "bytes", "maxTaskSlots", "reasonToCompact"),
        pendingCompactionTable.getColumnNames()
    );
    Assert.assertEquals(
        Collections.singletonList(
            Arrays.asList(
                dataSource,
                Intervals.of("2013-01-01/P1D"),
                1,
                100_000_000L,
                1,
                "not compacted yet"
            )
        ),
        pendingCompactionTable.getRows()
    );

    scheduler.stopCompaction(dataSource);

    final CompactionSimulateResult simulateResultWhenDisabled = scheduler.simulateRunWithConfigUpdate(
        new ClusterCompactionConfig(null, null, null, null, null)
    );
    Assert.assertTrue(simulateResultWhenDisabled.getCompactionStates().isEmpty());

    scheduler.stopBeingLeader();
  }

  @Test
  public void test_ingestHourGranularity_andCompactToDayAndMonth_withInlineTemplates()
  {
    final int numDays = (int) new Duration(MAR_11.getMillis() - JAN_20.getMillis()).getStandardDays();
    createSegments(24 * numDays, Granularities.HOUR, JAN_20);
    verifyNumSegmentsWith(Granularities.HOUR, 24 * numDays);

    // Compact everything going back to Mar 10 to DAY granularity, rest to MONTH
    final DateTime now = DateTimes.nowUtc();
    final Period dayRulePeriod = new Period(now.getMillis() - MAR_11.minusDays(1).minusMinutes(1).getMillis());
    CascadingCompactionTemplate cascadingTemplate = new CascadingCompactionTemplate(
        dataSource,
        List.of(
            new CompactionRule(dayRulePeriod, new InlineCompactionJobTemplate(createMatcher(Granularities.DAY))),
            new CompactionRule(Period.ZERO, new InlineCompactionJobTemplate(createMatcher(Granularities.MONTH)))
        )
    );

    startCompactionWithSpec(cascadingTemplate);
    runCompactionTasks(12);

    verifyFullyCompacted();
    verifyNumSegmentsWith(Granularities.HOUR, 0);
    verifyNumSegmentsWith(Granularities.DAY, 10);
    verifyNumSegmentsWith(Granularities.MONTH, 2);
  }

  @Test
  public void test_ingestHourGranularity_andCompactToDayAndMonth_withCatalogTemplates()
  {
    final int numDays = (int) new Duration(MAR_11.getMillis() - JAN_20.getMillis()).getStandardDays();
    createSegments(24 * numDays, Granularities.HOUR, JAN_20);
    verifyNumSegmentsWith(Granularities.HOUR, 24 * numDays);

    // Add compaction templates to catalog
    final String dayGranularityTemplateId = saveTemplateToCatalog(
        new InlineCompactionJobTemplate(createMatcher(Granularities.DAY))
    );
    final String monthGranularityTemplateId = saveTemplateToCatalog(
        new InlineCompactionJobTemplate(createMatcher(Granularities.MONTH))
    );

    // Compact everything going back to Mar 10 to DAY granularity, rest to MONTH
    final DateTime now = DateTimes.nowUtc();
    final Period dayRulePeriod = new Period(now.getMillis() - MAR_11.minusDays(1).minusMinutes(1).getMillis());
    CascadingCompactionTemplate cascadingTemplate = new CascadingCompactionTemplate(
        dataSource,
        List.of(
            new CompactionRule(dayRulePeriod, new CatalogCompactionJobTemplate(dayGranularityTemplateId, catalog)),
            new CompactionRule(Period.ZERO, new CatalogCompactionJobTemplate(monthGranularityTemplateId, catalog))
        )
    );

    startCompactionWithSpec(cascadingTemplate);
    runCompactionTasks(12);

    verifyFullyCompacted();
    verifyNumSegmentsWith(Granularities.HOUR, 0);
    verifyNumSegmentsWith(Granularities.DAY, 10);
    verifyNumSegmentsWith(Granularities.MONTH, 2);
  }

  @Test
  public void test_ingestHourGranularity_andCompactToDayAndMonth_withCatalogMSQTemplates()
  {
    dataSource = TestDataSource.WIKI;

    final int numDays = (int) new Duration(MAR_11.getMillis() - JAN_20.getMillis()).getStandardDays();
    createSegments(24 * numDays, Granularities.HOUR, JAN_20);
    verifyNumSegmentsWith(Granularities.HOUR, 24 * numDays);

    // Add compaction templates to catalog
    final String sqlDayGranularity =
        "REPLACE INTO ${dataSource}"
        + " OVERWRITE WHERE __time >= TIMESTAMP '${startTimestamp}' AND __time < TIMESTAMP '${endTimestamp}'"
        + " SELECT * FROM ${dataSource}"
        + " WHERE __time BETWEEN '${startTimestamp}' AND '${endTimestamp}'"
        + " PARTITIONED BY DAY";
    final String dayGranularityTemplateId = saveTemplateToCatalog(
        new MSQCompactionJobTemplate(
            new ClientSqlQuery(sqlDayGranularity, null, false, false, false, null, null),
            createMatcher(Granularities.DAY)
        )
    );
    final String sqlMonthGranularity =
        "REPLACE INTO ${dataSource}"
        + " OVERWRITE WHERE __time >= TIMESTAMP '${startTimestamp}' AND __time < TIMESTAMP '${endTimestamp}'"
        + " SELECT * FROM ${dataSource}"
        + " WHERE __time BETWEEN '${startTimestamp}' AND '${endTimestamp}'"
        + " PARTITIONED BY MONTH";
    final String monthGranularityTemplateId = saveTemplateToCatalog(
        new MSQCompactionJobTemplate(
            new ClientSqlQuery(sqlMonthGranularity, null, false, false, false, null, null),
            createMatcher(Granularities.MONTH)
        )
    );

    // Compact everything going back to Mar 10 to DAY granularity, rest to MONTH
    final DateTime now = DateTimes.nowUtc();
    final Period dayRulePeriod = new Period(now.getMillis() - MAR_11.minusDays(1).minusMinutes(1).getMillis());
    CascadingCompactionTemplate cascadingTemplate = new CascadingCompactionTemplate(
        dataSource,
        List.of(
            new CompactionRule(dayRulePeriod, new CatalogCompactionJobTemplate(dayGranularityTemplateId, catalog)),
            new CompactionRule(Period.ZERO, new CatalogCompactionJobTemplate(monthGranularityTemplateId, catalog))
        )
    );

    startCompactionWithSpec(cascadingTemplate);
    runMSQCompactionJobs(12);

    verifyFullyCompacted();
    verifyNumSegmentsWith(Granularities.HOUR, 0);
    verifyNumSegmentsWith(Granularities.DAY, 10);
    verifyNumSegmentsWith(Granularities.MONTH, 2);
  }

  private void verifyNumSegmentsWith(Granularity granularity, int numExpectedSegments)
  {
    long numMatchingSegments = segmentStorage
        .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
        .stream()
        .filter(segment -> granularity.isAligned(segment.getInterval()))
        .count();

    Assert.assertEquals(
        StringUtils.format("Segment with granularity[%s]", granularity),
        numExpectedSegments,
        (int) numMatchingSegments
    );
  }

  private void verifyFullyCompacted()
  {
    runScheduledJob();
    int numSegments = segmentStorage.retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE).size();

    final AutoCompactionSnapshot snapshot = scheduler.getCompactionSnapshot(dataSource);
    Assert.assertEquals(0, snapshot.getSegmentCountAwaitingCompaction());
    Assert.assertEquals(0, snapshot.getSegmentCountSkipped());
    Assert.assertEquals(numSegments, snapshot.getSegmentCountCompacted());
  }

  private void createSegments(int numSegments, Granularity granularity, DateTime firstSegmentStart)
  {
    final List<DataSegment> segments = CreateDataSegments
        .ofDatasource(dataSource)
        .forIntervals(numSegments, granularity)
        .startingAt(firstSegmentStart)
        .eachOfSizeInMb(100);
    segmentStorage.commitSegments(Set.copyOf(segments), null);
  }

  private String saveTemplateToCatalog(CompactionJobTemplate template)
  {
    final String templateId = IdUtils.getRandomId();
    final TableId tableId = TableId.of(TableId.INDEXING_TEMPLATE_SCHEMA, templateId);

    catalog.addSpec(
        tableId,
        new TableSpec(
            IndexingTemplateDefn.TYPE,
            Map.of(IndexingTemplateDefn.PROPERTY_PAYLOAD, template),
            null
        )
    );

    ResolvedTable table = catalog.resolveTable(tableId);
    Assert.assertNotNull(table);

    return templateId;
  }

  private void startCompactionWithSpec(DataSourceCompactionConfig config)
  {
    scheduler.becomeLeader();
    final CompactionSupervisorSpec compactionSupervisor
        = new CompactionSupervisorSpec(config, false, scheduler);
    scheduler.startCompaction(config.getDataSource(), compactionSupervisor.createSupervisor());
  }

  private void runCompactionTasks(int expectedCount)
  {
    runScheduledJob();
    serviceEmitter.verifySum("compact/task/count", expectedCount);

    ArgumentCaptor<Task> taskArgumentCaptor = ArgumentCaptor.forClass(Task.class);
    Mockito.verify(taskQueue, Mockito.times(expectedCount)).add(taskArgumentCaptor.capture());

    for (Task task : taskArgumentCaptor.getAllValues()) {
      Assert.assertTrue(task instanceof CompactionTask);
      Assert.assertEquals(dataSource, task.getDataSource());

      final CompactionTask compactionTask = (CompactionTask) task;
      runCompactionTask(
          compactionTask.getId(),
          compactionTask.getIoConfig().getInputSpec().findInterval(dataSource),
          compactionTask.getSegmentGranularity()
      );
    }

    segmentStorage.getManager().forceUpdateDataSourcesSnapshot();
  }

  private void runCompactionTask(String taskId, Interval compactionInterval, Granularity segmentGranularity)
  {
    // Update status of task in TaskQueue
    Mockito.when(taskQueue.getTaskStatus(taskId))
           .thenReturn(Optional.of(TaskStatus.success(taskId)));

    // Determine interval and granularity and apply it to the timeline
    if (segmentGranularity == null) {
      // Nothing to do
      return;
    }

    for (Interval replaceInterval : segmentGranularity.getIterable(compactionInterval)) {
      // Create a single segment in this interval
      DataSegment replaceSegment = CreateDataSegments
          .ofDatasource(dataSource)
          .forIntervals(1, segmentGranularity)
          .startingAt(replaceInterval.getStart())
          .withVersion("2")
          .eachOfSizeInMb(100)
          .get(0);
      segmentStorage.commitSegments(Set.of(replaceSegment), null);
    }
  }

  private void runMSQCompactionJobs(int numExpectedJobs)
  {
    runScheduledJob();
    serviceEmitter.verifySum("compact/task/count", numExpectedJobs);

    ArgumentCaptor<ClientSqlQuery> queryArgumentCaptor = ArgumentCaptor.forClass(ClientSqlQuery.class);
    Mockito.verify(brokerClient, Mockito.times(numExpectedJobs))
           .submitSqlTask(queryArgumentCaptor.capture());

    for (ClientSqlQuery job : queryArgumentCaptor.getAllValues()) {
      final String query = job.getQuery();

      final Granularity segmentGranularity;
      if (query.contains("PARTITIONED BY DAY")) {
        segmentGranularity = Granularities.DAY;
      } else if (query.contains("PARTITIONED BY MONTH")) {
        segmentGranularity = Granularities.MONTH;
      } else {
        segmentGranularity = Granularities.HOUR;
      }

      final Interval compactionInterval =
          (Interval) job.getContext().get(CompactSegments.COMPACTION_INTERVAL_KEY);
      runCompactionTask(
          submittedMsqTaskIds.get(compactionInterval),
          compactionInterval,
          segmentGranularity
      );
    }

    segmentStorage.getManager().forceUpdateDataSourcesSnapshot();
  }

  private static CompactionStateMatcher createMatcher(Granularity segmentGranularity)
  {
    return new CompactionStateMatcher(
        null,
        null,
        null,
        null,
        null,
        new UserCompactionTaskGranularityConfig(segmentGranularity, null, null),
        null
    );
  }

  private void disableScheduler()
  {
    compactionConfig.set(new ClusterCompactionConfig(null, null, null, false, null));
  }

  private void enableScheduler()
  {
    compactionConfig.set(new ClusterCompactionConfig(null, null, null, true, null));
  }

  private void runScheduledJob()
  {
    executor.finishNextPendingTask();
  }

  private CompactionSupervisor createSupervisorWithInlineSpec()
  {
    return new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .build(),
        false,
        scheduler
    ).createSupervisor();
  }
}

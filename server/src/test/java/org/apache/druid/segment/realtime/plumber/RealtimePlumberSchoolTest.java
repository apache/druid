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

package org.apache.druid.segment.realtime.plumber;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.handoff.SegmentHandoffNotifier;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.FireDepartmentTest;
import org.apache.druid.segment.realtime.FireHydrant;
import org.apache.druid.segment.realtime.SegmentPublisher;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@RunWith(Parameterized.class)
public class RealtimePlumberSchoolTest extends InitializedNullHandlingTest
{
  @Parameterized.Parameters(name = "rejectionPolicy = {0}, segmentWriteOutMediumFactory = {1}")
  public static Collection<?> constructorFeeder()
  {
    final RejectionPolicyFactory[] rejectionPolicies = new RejectionPolicyFactory[]{
        new NoopRejectionPolicyFactory(),
        new MessageTimeRejectionPolicyFactory()
    };

    final List<Object[]> constructors = new ArrayList<>();
    for (RejectionPolicyFactory rejectionPolicy : rejectionPolicies) {
      constructors.add(new Object[]{rejectionPolicy, OffHeapMemorySegmentWriteOutMediumFactory.instance()});
      constructors.add(new Object[]{rejectionPolicy, TmpFileSegmentWriteOutMediumFactory.instance()});
    }
    return constructors;
  }

  private final RejectionPolicyFactory rejectionPolicy;
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
  private RealtimePlumber plumber;
  private RealtimePlumberSchool realtimePlumberSchool;
  private DataSegmentAnnouncer announcer;
  private SegmentPublisher segmentPublisher;
  private DataSegmentPusher dataSegmentPusher;
  private SegmentHandoffNotifier handoffNotifier;
  private SegmentHandoffNotifierFactory handoffNotifierFactory;
  private ServiceEmitter emitter;
  private RealtimeTuningConfig tuningConfig;
  private DataSchema schema;
  private DataSchema schema2;
  private FireDepartmentMetrics metrics;
  private File tmpDir;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public RealtimePlumberSchoolTest(
      RejectionPolicyFactory rejectionPolicy,
      SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  )
  {
    this.rejectionPolicy = rejectionPolicy;
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
  }

  @Before
  public void setUp() throws Exception
  {
    tmpDir = FileUtils.createTempDir();

    ObjectMapper jsonMapper = new DefaultObjectMapper();

    schema = new DataSchema(
        "test",
        jsonMapper.convertValue(
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec("timestamp", "auto", null),
                    DimensionsSpec.EMPTY,
                    null,
                    null,
                    null
                ),
                null
            ),
            Map.class
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularities.HOUR, Granularities.NONE, null),
        null,
        jsonMapper
    );

    schema2 = new DataSchema(
        "test",
        jsonMapper.convertValue(
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec("timestamp", "auto", null),
                    DimensionsSpec.EMPTY,
                    null,
                    null,
                    null
                ),
                null
            ),
            Map.class
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularities.YEAR, Granularities.NONE, null),
        null,
        jsonMapper
    );

    announcer = EasyMock.createMock(DataSegmentAnnouncer.class);
    announcer.announceSegment(EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();

    segmentPublisher = EasyMock.createNiceMock(SegmentPublisher.class);
    dataSegmentPusher = EasyMock.createNiceMock(DataSegmentPusher.class);
    handoffNotifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    handoffNotifier = EasyMock.createNiceMock(SegmentHandoffNotifier.class);
    EasyMock.expect(handoffNotifierFactory.createSegmentHandoffNotifier(EasyMock.anyString()))
            .andReturn(handoffNotifier)
            .anyTimes();
    EasyMock.expect(
        handoffNotifier.registerSegmentHandoffCallback(
            EasyMock.anyObject(),
            EasyMock.anyObject(),
            EasyMock.anyObject()
        )
    ).andReturn(true).anyTimes();

    emitter = EasyMock.createMock(ServiceEmitter.class);

    EasyMock.replay(announcer, segmentPublisher, dataSegmentPusher, handoffNotifierFactory, handoffNotifier, emitter);

    tuningConfig = new RealtimeTuningConfig(
        null,
        1,
        null,
        null,
        null,
        null,
        temporaryFolder.newFolder(),
        new IntervalStartVersioningPolicy(),
        rejectionPolicy,
        null,
        null,
        null,
        null,
        0,
        0,
        false,
        null,
        null,
        null,
        null,
        null
    );

    realtimePlumberSchool = new RealtimePlumberSchool(
        emitter,
        new DefaultQueryRunnerFactoryConglomerate(new HashMap<>()),
        dataSegmentPusher,
        announcer,
        segmentPublisher,
        handoffNotifierFactory,
        DirectQueryProcessingPool.INSTANCE,
        NoopJoinableFactory.INSTANCE,
        TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory),
        TestHelper.getTestIndexIO(),
        MapCache.create(0),
        FireDepartmentTest.NO_CACHE_CONFIG,
        new CachePopulatorStats(),
        TestHelper.makeJsonMapper()
    );

    metrics = new FireDepartmentMetrics();
    plumber = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema, tuningConfig, metrics);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(announcer, segmentPublisher, dataSegmentPusher, handoffNotifierFactory, handoffNotifier, emitter);
    FileUtils.deleteDirectory(
        new File(
            tuningConfig.getBasePersistDirectory(),
            schema.getDataSource()
        )
    );
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test(timeout = 60_000L)
  public void testPersist() throws Exception
  {
    testPersist(null);
  }

  @Test(timeout = 60_000L)
  public void testPersistWithCommitMetadata() throws Exception
  {
    final Object commitMetadata = "dummyCommitMetadata";
    testPersist(commitMetadata);

    plumber = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema, tuningConfig, metrics);
    Assert.assertEquals(commitMetadata, plumber.startJob());
  }

  private void testPersist(final Object commitMetadata) throws Exception
  {
    Sink sink = new Sink(
        Intervals.utc(0, TimeUnit.HOURS.toMillis(1)),
        schema,
        tuningConfig.getShardSpec(),
        DateTimes.of("2014-12-01T12:34:56.789").toString(),
        tuningConfig.getAppendableIndexSpec(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxBytesInMemoryOrDefault(),
        true,
        tuningConfig.getDedupColumn()
    );
    plumber.getSinks().put(0L, sink);
    Assert.assertNull(plumber.startJob());

    final InputRow row = EasyMock.createNiceMock(InputRow.class);
    EasyMock.expect(row.getTimestampFromEpoch()).andReturn(0L);
    EasyMock.expect(row.getDimensions()).andReturn(new ArrayList<String>());
    EasyMock.replay(row);

    final CountDownLatch doneSignal = new CountDownLatch(1);

    final Committer committer = new Committer()
    {
      @Override
      public Object getMetadata()
      {
        return commitMetadata;
      }

      @Override
      public void run()
      {
        doneSignal.countDown();
      }
    };
    plumber.add(row, Suppliers.ofInstance(committer));
    plumber.persist(committer);

    doneSignal.await();

    plumber.getSinks().clear();
    plumber.finishJob();
  }

  @Test(timeout = 60_000L)
  public void testPersistFails() throws Exception
  {
    Sink sink = new Sink(
        Intervals.utc(0, TimeUnit.HOURS.toMillis(1)),
        schema,
        tuningConfig.getShardSpec(),
        DateTimes.of("2014-12-01T12:34:56.789").toString(),
        tuningConfig.getAppendableIndexSpec(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxBytesInMemoryOrDefault(),
        true,
        tuningConfig.getDedupColumn()
    );
    plumber.getSinks().put(0L, sink);
    plumber.startJob();
    final InputRow row = EasyMock.createNiceMock(InputRow.class);
    EasyMock.expect(row.getTimestampFromEpoch()).andReturn(0L);
    EasyMock.expect(row.getDimensions()).andReturn(new ArrayList<String>());
    EasyMock.replay(row);
    plumber.add(row, Suppliers.ofInstance(Committers.nil()));

    final CountDownLatch doneSignal = new CountDownLatch(1);

    plumber.persist(
        supplierFromRunnable(
            () -> {
              doneSignal.countDown();
              throw new RuntimeException();
            }
        ).get()
    );

    doneSignal.await();

    // Exception may need time to propagate
    while (metrics.failedPersists() < 1) {
      Thread.sleep(100);
    }

    Assert.assertEquals(1, metrics.failedPersists());
  }

  @Test(timeout = 60_000L)
  public void testPersistHydrantGaps() throws Exception
  {
    final Object commitMetadata = "dummyCommitMetadata";
    testPersistHydrantGapsHelper(commitMetadata);
  }

  private void testPersistHydrantGapsHelper(final Object commitMetadata) throws Exception
  {
    Interval testInterval = new Interval(DateTimes.of("1970-01-01"), DateTimes.of("1971-01-01"));

    RealtimePlumber plumber2 = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema2, tuningConfig, metrics);
    Sink sink = new Sink(
        testInterval,
        schema2,
        tuningConfig.getShardSpec(),
        DateTimes.of("2014-12-01T12:34:56.789").toString(),
        tuningConfig.getAppendableIndexSpec(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxBytesInMemoryOrDefault(),
        true,
        tuningConfig.getDedupColumn()
    );
    plumber2.getSinks().put(0L, sink);
    Assert.assertNull(plumber2.startJob());
    final CountDownLatch doneSignal = new CountDownLatch(1);
    final Committer committer = new Committer()
    {
      @Override
      public Object getMetadata()
      {
        return commitMetadata;
      }

      @Override
      public void run()
      {
        doneSignal.countDown();
      }
    };
    plumber2.add(getTestInputRow("1970-01-01"), Suppliers.ofInstance(committer));
    plumber2.add(getTestInputRow("1970-02-01"), Suppliers.ofInstance(committer));
    plumber2.add(getTestInputRow("1970-03-01"), Suppliers.ofInstance(committer));
    plumber2.add(getTestInputRow("1970-04-01"), Suppliers.ofInstance(committer));
    plumber2.add(getTestInputRow("1970-05-01"), Suppliers.ofInstance(committer));

    plumber2.persist(committer);

    doneSignal.await();
    plumber2.getSinks().clear();
    plumber2.finishJob();

    File persistDir = plumber2.computePersistDir(schema2, testInterval);

    /* Check that all hydrants were persisted */
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(new File(persistDir, String.valueOf(i)).exists());
    }

    /* Create some gaps in the persisted hydrants and reload */
    FileUtils.deleteDirectory(new File(persistDir, "1"));
    FileUtils.deleteDirectory(new File(persistDir, "3"));
    RealtimePlumber restoredPlumber = (RealtimePlumber) realtimePlumberSchool.findPlumber(
        schema2,
        tuningConfig,
        metrics
    );
    restoredPlumber.bootstrapSinksFromDisk();

    Map<Long, Sink> sinks = restoredPlumber.getSinks();
    Assert.assertEquals(1, sinks.size());


    List<FireHydrant> hydrants = Lists.newArrayList(sinks.get(new Long(0)));
    DateTime startTime = DateTimes.of("1970-01-01T00:00:00.000Z");
    Interval expectedInterval = new Interval(startTime, DateTimes.of("1971-01-01T00:00:00.000Z"));
    Assert.assertEquals(0, hydrants.get(0).getCount());
    Assert.assertEquals(
        expectedInterval,
        hydrants.get(0).getSegmentDataInterval()
    );
    Assert.assertEquals(2, hydrants.get(1).getCount());
    Assert.assertEquals(
        expectedInterval,
        hydrants.get(1).getSegmentDataInterval()
    );
    Assert.assertEquals(4, hydrants.get(2).getCount());
    Assert.assertEquals(
        expectedInterval,
        hydrants.get(2).getSegmentDataInterval()
    );

    /* Delete all the hydrants and reload, no sink should be created */
    FileUtils.deleteDirectory(new File(persistDir, "0"));
    FileUtils.deleteDirectory(new File(persistDir, "2"));
    FileUtils.deleteDirectory(new File(persistDir, "4"));
    RealtimePlumber restoredPlumber2 = (RealtimePlumber) realtimePlumberSchool.findPlumber(
        schema2,
        tuningConfig,
        metrics
    );
    restoredPlumber2.bootstrapSinksFromDisk();

    Assert.assertEquals(0, restoredPlumber2.getSinks().size());
  }

  @Test(timeout = 60_000L)
  public void testDimOrderInheritance() throws Exception
  {
    final Object commitMetadata = "dummyCommitMetadata";
    testDimOrderInheritanceHelper(commitMetadata);
  }

  private void testDimOrderInheritanceHelper(final Object commitMetadata) throws Exception
  {
    List<List<String>> expectedDims = ImmutableList.of(
        ImmutableList.of("dimD"),
        ImmutableList.of("dimC"),
        ImmutableList.of("dimA"),
        ImmutableList.of("dimB"),
        ImmutableList.of("dimE"),
        ImmutableList.of("dimD", "dimC", "dimA", "dimB", "dimE")
    );

    QueryableIndex qindex;
    FireHydrant hydrant;
    Map<Long, Sink> sinks;

    RealtimePlumber plumber = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema2, tuningConfig, metrics);
    Assert.assertNull(plumber.startJob());

    final CountDownLatch doneSignal = new CountDownLatch(1);

    final Committer committer = new Committer()
    {
      @Override
      public Object getMetadata()
      {
        return commitMetadata;
      }

      @Override
      public void run()
      {
        doneSignal.countDown();
      }
    };

    plumber.add(
        getTestInputRowFull(
            "1970-01-01",
            ImmutableList.of("dimD"),
            ImmutableList.of("1")
        ),
        Suppliers.ofInstance(committer)
    );
    plumber.add(
        getTestInputRowFull(
            "1970-01-01",
            ImmutableList.of("dimC"),
            ImmutableList.of("1")
        ),
        Suppliers.ofInstance(committer)
    );
    plumber.add(
        getTestInputRowFull(
            "1970-01-01",
            ImmutableList.of("dimA"),
            ImmutableList.of("1")
        ),
        Suppliers.ofInstance(committer)
    );
    plumber.add(
        getTestInputRowFull(
            "1970-01-01",
            ImmutableList.of("dimB"),
            ImmutableList.of("1")
        ),
        Suppliers.ofInstance(committer)
    );
    plumber.add(
        getTestInputRowFull(
            "1970-01-01",
            ImmutableList.of("dimE"),
            ImmutableList.of("1")
        ),
        Suppliers.ofInstance(committer)
    );
    plumber.add(
        getTestInputRowFull(
            "1970-01-01",
            ImmutableList.of("dimA", "dimB", "dimC", "dimD", "dimE"),
            ImmutableList.of("1")
        ),
        Suppliers.ofInstance(committer)
    );

    plumber.persist(committer);

    doneSignal.await();

    plumber.getSinks().clear();
    plumber.finishJob();

    RealtimePlumber restoredPlumber = (RealtimePlumber) realtimePlumberSchool.findPlumber(
        schema2,
        tuningConfig,
        metrics
    );
    restoredPlumber.bootstrapSinksFromDisk();

    sinks = restoredPlumber.getSinks();
    Assert.assertEquals(1, sinks.size());
    List<FireHydrant> hydrants = Lists.newArrayList(sinks.get(0L));

    for (int i = 0; i < hydrants.size(); i++) {
      hydrant = hydrants.get(i);
      ReferenceCountingSegment segment = hydrant.getIncrementedSegment();
      try {
        qindex = segment.asQueryableIndex();
        Assert.assertEquals(i, hydrant.getCount());
        Assert.assertEquals(expectedDims.get(i), ImmutableList.copyOf(qindex.getAvailableDimensions()));
      }
      finally {
        segment.decrement();
      }
    }
  }

  private InputRow getTestInputRow(final String timeStr)
  {
    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return new ArrayList<>();
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return DateTimes.of(timeStr).getMillis();
      }

      @Override
      public DateTime getTimestamp()
      {
        return DateTimes.of(timeStr);
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        return new ArrayList<>();
      }

      @Override
      public Number getMetric(String metric)
      {
        return 0;
      }

      @Override
      public Object getRaw(String dimension)
      {
        return null;
      }

      @Override
      public int compareTo(Row o)
      {
        return 0;
      }
    };
  }

  private InputRow getTestInputRowFull(final String timeStr, final List<String> dims, final List<String> dimVals)
  {
    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return dims;
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return DateTimes.of(timeStr).getMillis();
      }

      @Override
      public DateTime getTimestamp()
      {
        return DateTimes.of(timeStr);
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        return dimVals;
      }

      @Override
      public Number getMetric(String metric)
      {
        return 0;
      }

      @Override
      public Object getRaw(String dimension)
      {
        return dimVals;
      }

      @Override
      public int compareTo(Row o)
      {
        return 0;
      }
    };
  }

  private static Supplier<Committer> supplierFromRunnable(final Runnable runnable)
  {
    final Committer committer = new Committer()
    {
      @Override
      public Object getMetadata()
      {
        return null;
      }

      @Override
      public void run()
      {
        runnable.run();
      }
    };
    return Suppliers.ofInstance(committer);
  }
}

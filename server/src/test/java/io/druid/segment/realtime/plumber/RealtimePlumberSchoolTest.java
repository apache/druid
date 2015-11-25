/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.segment.realtime.plumber;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Granularity;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.FilteredServerView;
import io.druid.client.ServerView;
import io.druid.client.cache.MapCache;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.TestHelper;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireDepartmentTest;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
@RunWith(Parameterized.class)
public class RealtimePlumberSchoolTest
{
  private final RejectionPolicyFactory rejectionPolicy;
  private RealtimePlumber plumber;
  private RealtimePlumberSchool realtimePlumberSchool;
  private DataSegmentAnnouncer announcer;
  private SegmentPublisher segmentPublisher;
  private DataSegmentPusher dataSegmentPusher;
  private SegmentHandoffNotifierFactory handoffNotifierFactory;
  private SegmentHandoffNotifier handoffNotifier;
  private ServiceEmitter emitter;
  private RealtimeTuningConfig tuningConfig;
  private DataSchema schema;
  private DataSchema schema2;
  private FireDepartmentMetrics metrics;

  public RealtimePlumberSchoolTest(RejectionPolicyFactory rejectionPolicy)
  {
    this.rejectionPolicy = rejectionPolicy;
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return Arrays.asList(
        new Object[][]{
            {
                new NoopRejectionPolicyFactory()
            },
            {
                new MessageTimeRejectionPolicyFactory()
            }
        }
    );
  }

  @Before
  public void setUp() throws Exception
  {
    final File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    ObjectMapper jsonMapper = new DefaultObjectMapper();

    schema = new DataSchema(
        "test",
        jsonMapper.convertValue(
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec("timestamp", "auto", null),
                    new DimensionsSpec(null, null, null)
                )
            ),
            Map.class
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularity.HOUR, QueryGranularity.NONE, null),
        jsonMapper
    );

    schema2 = new DataSchema(
        "test",
        jsonMapper.convertValue(
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec("timestamp", "auto", null),
                    new DimensionsSpec(null, null, null)
                )
            ),
            Map.class
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularity.YEAR, QueryGranularity.NONE, null),
        jsonMapper
    );

    announcer = EasyMock.createMock(DataSegmentAnnouncer.class);
    announcer.announceSegment(EasyMock.<DataSegment>anyObject());
    EasyMock.expectLastCall().anyTimes();

    segmentPublisher = EasyMock.createNiceMock(SegmentPublisher.class);
    dataSegmentPusher = EasyMock.createNiceMock(DataSegmentPusher.class);
    handoffNotifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    handoffNotifier = EasyMock.createNiceMock(SegmentHandoffNotifier.class);
    EasyMock.expect(handoffNotifierFactory.createSegmentHandoffNotifier(EasyMock.anyString())).andReturn(handoffNotifier).anyTimes();
    handoffNotifier.registerSegmentHandoffCallback(EasyMock.<SegmentDescriptor>anyObject(),
                                            EasyMock.<Executor>anyObject(),
                                            EasyMock.<Runnable>anyObject()
    );
    EasyMock.expectLastCall().anyTimes();

    emitter = EasyMock.createMock(ServiceEmitter.class);

    EasyMock.replay(announcer, segmentPublisher, dataSegmentPusher, handoffNotifierFactory, handoffNotifier, emitter);

    tuningConfig = new RealtimeTuningConfig(
        1,
        null,
        null,
        null,
        new IntervalStartVersioningPolicy(),
        rejectionPolicy,
        null,
        null,
        null
    );

    realtimePlumberSchool = new RealtimePlumberSchool(
        emitter,
        new DefaultQueryRunnerFactoryConglomerate(Maps.<Class<? extends Query>, QueryRunnerFactory>newHashMap()),
        dataSegmentPusher,
        announcer,
        segmentPublisher,
        handoffNotifierFactory,
        MoreExecutors.sameThreadExecutor(),
        TestHelper.getTestIndexMerger(),
        TestHelper.getTestIndexIO(),
        MapCache.create(0),
        FireDepartmentTest.NO_CACHE_CONFIG,
        TestHelper.getObjectMapper()
    );

    metrics = new FireDepartmentMetrics();
    plumber = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema, tuningConfig, metrics);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(announcer, segmentPublisher, dataSegmentPusher, handoffNotifierFactory, emitter);
    FileUtils.deleteDirectory(
        new File(
            tuningConfig.getBasePersistDirectory(),
            schema.getDataSource()
        )
    );
  }

  @Test(timeout = 60000)
  public void testPersist() throws Exception
  {
    testPersist(null);
  }

  @Test(timeout = 60000)
  public void testPersistWithCommitMetadata() throws Exception
  {
    final Object commitMetadata = "dummyCommitMetadata";
    testPersist(commitMetadata);

    plumber = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema, tuningConfig, metrics);
    Assert.assertEquals(commitMetadata, plumber.startJob());
  }

  private void testPersist(final Object commitMetadata) throws Exception
  {
    final AtomicBoolean committed = new AtomicBoolean(false);
    plumber.getSinks()
           .put(
               0L,
               new Sink(
                   new Interval(0, TimeUnit.HOURS.toMillis(1)),
                   schema,
                   tuningConfig,
                   new DateTime("2014-12-01T12:34:56.789").toString()
               )
           );
    Assert.assertNull(plumber.startJob());

    final InputRow row = EasyMock.createNiceMock(InputRow.class);
    EasyMock.expect(row.getTimestampFromEpoch()).andReturn(0L);
    EasyMock.expect(row.getDimensions()).andReturn(new ArrayList<String>());
    EasyMock.replay(row);
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
        committed.set(true);
      }
    };
    plumber.add(row, Suppliers.ofInstance(committer));
    plumber.persist(committer);

    while (!committed.get()) {
      Thread.sleep(100);
    }
    plumber.getSinks().clear();
    plumber.finishJob();
  }

  @Test(timeout = 60000)
  public void testPersistFails() throws Exception
  {
    final AtomicBoolean committed = new AtomicBoolean(false);
    plumber.getSinks()
           .put(
               0L,
               new Sink(
                   new Interval(0, TimeUnit.HOURS.toMillis(1)),
                   schema,
                   tuningConfig,
                   new DateTime("2014-12-01T12:34:56.789").toString()
               )
           );
    plumber.startJob();
    final InputRow row = EasyMock.createNiceMock(InputRow.class);
    EasyMock.expect(row.getTimestampFromEpoch()).andReturn(0L);
    EasyMock.expect(row.getDimensions()).andReturn(new ArrayList<String>());
    EasyMock.replay(row);
    plumber.add(row, Committers.supplierOf(Committers.nil()));
    plumber.persist(
        Committers.supplierFromRunnable(
            new Runnable()
            {
              @Override
              public void run()
              {
                committed.set(true);
                throw new RuntimeException();
              }
            }
        ).get()
    );
    while (!committed.get()) {
      Thread.sleep(100);
    }

    // Exception may need time to propagate
    while (metrics.failedPersists() < 1) {
      Thread.sleep(100);
    }

    Assert.assertEquals(1, metrics.failedPersists());
  }

  @Test(timeout = 60000)
  public void testPersistHydrantGaps() throws Exception
  {
    final Object commitMetadata = "dummyCommitMetadata";
    testPersistHydrantGapsHelper(commitMetadata);
  }

  private void testPersistHydrantGapsHelper(final Object commitMetadata) throws Exception
  {
    final AtomicBoolean committed = new AtomicBoolean(false);
    Interval testInterval = new Interval(new DateTime("1970-01-01"), new DateTime("1971-01-01"));

    RealtimePlumber plumber2 = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema2, tuningConfig, metrics);
    plumber2.getSinks()
           .put(
               0L,
               new Sink(
                   testInterval,
                   schema2,
                   tuningConfig,
                   new DateTime("2014-12-01T12:34:56.789").toString()
               )
           );
    Assert.assertNull(plumber2.startJob());

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
        committed.set(true);
      }
    };
    plumber2.add(getTestInputRow("1970-01-01"), Suppliers.ofInstance(committer));
    plumber2.add(getTestInputRow("1970-02-01"), Suppliers.ofInstance(committer));
    plumber2.add(getTestInputRow("1970-03-01"), Suppliers.ofInstance(committer));
    plumber2.add(getTestInputRow("1970-04-01"), Suppliers.ofInstance(committer));
    plumber2.add(getTestInputRow("1970-05-01"), Suppliers.ofInstance(committer));

    plumber2.persist(committer);

    while (!committed.get()) {
      Thread.sleep(100);
    }
    plumber2.getSinks().clear();
    plumber2.finishJob();

    File persistDir = plumber2.computePersistDir(schema2, testInterval);

    /* Check that all hydrants were persisted */
    for (int i = 0; i < 5; i ++) {
      Assert.assertTrue(new File(persistDir, String.valueOf(i)).exists());
    }

    /* Create some gaps in the persisted hydrants and reload */
    FileUtils.deleteDirectory(new File(persistDir, "1"));
    FileUtils.deleteDirectory(new File(persistDir, "3"));
    RealtimePlumber restoredPlumber = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema2, tuningConfig, metrics);
    restoredPlumber.bootstrapSinksFromDisk();

    Map<Long, Sink> sinks = restoredPlumber.getSinks();
    Assert.assertEquals(1, sinks.size());

    List<FireHydrant> hydrants = Lists.newArrayList(sinks.get(new Long(0)));
    DateTime startTime = new DateTime("1970-01-01T00:00:00.000Z");
    Assert.assertEquals(0, hydrants.get(0).getCount());
    Assert.assertEquals(new Interval(startTime, new DateTime("1970-01-01T00:00:00.001Z")),
                        hydrants.get(0).getSegment().getDataInterval());
    Assert.assertEquals(2, hydrants.get(1).getCount());
    Assert.assertEquals(new Interval(startTime, new DateTime("1970-03-01T00:00:00.001Z")),
                        hydrants.get(1).getSegment().getDataInterval());
    Assert.assertEquals(4, hydrants.get(2).getCount());
    Assert.assertEquals(new Interval(startTime, new DateTime("1970-05-01T00:00:00.001Z")),
                        hydrants.get(2).getSegment().getDataInterval());

    /* Delete all the hydrants and reload, no sink should be created */
    FileUtils.deleteDirectory(new File(persistDir, "0"));
    FileUtils.deleteDirectory(new File(persistDir, "2"));
    FileUtils.deleteDirectory(new File(persistDir, "4"));
    RealtimePlumber restoredPlumber2 = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema2, tuningConfig, metrics);
    restoredPlumber2.bootstrapSinksFromDisk();

    Assert.assertEquals(0, restoredPlumber2.getSinks().size());
  }

  private InputRow getTestInputRow(final String timeStr) {
    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return Lists.newArrayList();
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return new DateTime(timeStr).getMillis();
      }

      @Override
      public DateTime getTimestamp()
      {
        return new DateTime(timeStr);
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        return Lists.newArrayList();
      }

      @Override
      public float getFloatMetric(String metric)
      {
        return 0;
      }

      @Override
      public long getLongMetric(String metric)
      {
        return 0L;
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

}

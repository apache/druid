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

package org.apache.druid.segment.realtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.FirehoseFactoryV2;
import org.apache.druid.data.input.FirehoseV2;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.TuningConfigs;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.plumber.Plumber;
import org.apache.druid.segment.realtime.plumber.Sink;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.utils.Runnables;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RealtimeManagerTest
{
  private static QueryRunnerFactory factory;
  private static Closer resourceCloser;
  private static QueryRunnerFactoryConglomerate conglomerate;

  private static final List<TestInputRowHolder> rows = Arrays.asList(
      makeRow(DateTimes.of("9000-01-01").getMillis()),
      makeRow(new ParseException("parse error")),
      null,
      makeRow(System.currentTimeMillis())
  );

  private RealtimeManager realtimeManager;
  private RealtimeManager realtimeManager2;
  private RealtimeManager realtimeManager3;
  private DataSchema schema;
  private DataSchema schema2;
  private TestPlumber plumber;
  private TestPlumber plumber2;
  private RealtimeTuningConfig tuningConfig_0;
  private RealtimeTuningConfig tuningConfig_1;
  private DataSchema schema3;

  @BeforeClass
  public static void setupStatic()
  {
    final Pair<GroupByQueryRunnerFactory, Closer> factoryAndCloser = initFactory();
    factory = factoryAndCloser.lhs;
    resourceCloser = factoryAndCloser.rhs;
    conglomerate = new QueryRunnerFactoryConglomerate()
    {
      @Override
      public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query)
      {
        return factory;
      }
    };
  }

  @AfterClass
  public static void teardownStatic() throws IOException
  {
    resourceCloser.close();
  }

  @Before
  public void setUp()
  {
    ObjectMapper jsonMapper = new DefaultObjectMapper();

    schema = new DataSchema(
        "test",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularities.HOUR, Granularities.NONE, null),
        null,
        jsonMapper
    );
    schema2 = new DataSchema(
        "testV2",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularities.HOUR, Granularities.NONE, null),
        null,
        jsonMapper
    );
    RealtimeIOConfig ioConfig = new RealtimeIOConfig(
        new FirehoseFactory()
        {
          @Override
          public Firehose connect(InputRowParser parser, File temporaryDirectory)
          {
            return new TestFirehose(rows.iterator());
          }
        },
        (schema, config, metrics) -> plumber,
        null
    );
    RealtimeIOConfig ioConfig2 = new RealtimeIOConfig(
        null,
        (schema, config, metrics) -> plumber2,
        new FirehoseFactoryV2()
        {
          @Override
          public FirehoseV2 connect(InputRowParser parser, Object arg1) throws ParseException
          {
            return new TestFirehoseV2(rows.iterator());
          }
        }
    );
    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        1,
        null,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        0,
        null,
        null,
        null,
        null,
        null
    );
    plumber = new TestPlumber(new Sink(
        Intervals.of("0/P5000Y"),
        schema,
        tuningConfig.getShardSpec(),
        DateTimes.nowUtc().toString(),
        tuningConfig.getMaxRowsInMemory(),
        TuningConfigs.getMaxBytesInMemoryOrDefault(tuningConfig.getMaxBytesInMemory()),
        tuningConfig.isReportParseExceptions(),
        tuningConfig.getDedupColumn()
    ));

    realtimeManager = new RealtimeManager(
        Collections.singletonList(
            new FireDepartment(
                schema,
                ioConfig,
                tuningConfig
            )
        ),
        null,
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class)
    );
    plumber2 = new TestPlumber(new Sink(
        Intervals.of("0/P5000Y"),
        schema2,
        tuningConfig.getShardSpec(),
        DateTimes.nowUtc().toString(),
        tuningConfig.getMaxRowsInMemory(),
        TuningConfigs.getMaxBytesInMemoryOrDefault(tuningConfig.getMaxBytesInMemory()),
        tuningConfig.isReportParseExceptions(),
        tuningConfig.getDedupColumn()
    ));

    realtimeManager2 = new RealtimeManager(
        Collections.singletonList(
            new FireDepartment(
                schema2,
                ioConfig2,
                tuningConfig
            )
        ),
        null,
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class)
    );

    tuningConfig_0 = new RealtimeTuningConfig(
        1,
        null,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        null,
        new LinearShardSpec(0),
        null,
        null,
        0,
        0,
        null,
        null,
        null,
        null,
        null
    );

    tuningConfig_1 = new RealtimeTuningConfig(
        1,
        null,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        null,
        new LinearShardSpec(1),
        null,
        null,
        0,
        0,
        null,
        null,
        null,
        null,
        null
    );

    schema3 = new DataSchema(
        "testing",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("ignore")},
        new UniformGranularitySpec(Granularities.HOUR, Granularities.NONE, null),
        null,
        jsonMapper
    );

    FireDepartment department_0 = new FireDepartment(schema3, ioConfig, tuningConfig_0);
    FireDepartment department_1 = new FireDepartment(schema3, ioConfig2, tuningConfig_1);

    realtimeManager3 = new RealtimeManager(
        Arrays.asList(department_0, department_1),
        conglomerate,
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        null
    );
  }

  @After
  public void tearDown()
  {
    realtimeManager.stop();
    realtimeManager2.stop();
    realtimeManager3.stop();
  }

  @Test
  public void testRun() throws Exception
  {
    realtimeManager.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    while (realtimeManager.getMetrics("test").processed() != 1) {
      Thread.sleep(100);
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > 1000) {
        throw new ISE("Realtime manager should have completed processing 2 events!");
      }
    }

    Assert.assertEquals(1, realtimeManager.getMetrics("test").processed());
    Assert.assertEquals(2, realtimeManager.getMetrics("test").thrownAway());
    Assert.assertEquals(1, realtimeManager.getMetrics("test").unparseable());
    Assert.assertTrue(plumber.isStartedJob());
    Assert.assertTrue(plumber.isFinishedJob());
    Assert.assertEquals(0, plumber.getPersistCount());
  }

  @Test
  public void testRunV2() throws Exception
  {
    realtimeManager2.start();

    Stopwatch stopwatch = Stopwatch.createStarted();
    while (realtimeManager2.getMetrics("testV2").processed() != 1) {
      Thread.sleep(100);
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > 1000) {
        throw new ISE("Realtime manager should have completed processing 2 events!");
      }
    }

    Assert.assertEquals(1, realtimeManager2.getMetrics("testV2").processed());
    Assert.assertEquals(1, realtimeManager2.getMetrics("testV2").thrownAway());
    Assert.assertEquals(2, realtimeManager2.getMetrics("testV2").unparseable());
    Assert.assertTrue(plumber2.isStartedJob());
    Assert.assertTrue(plumber2.isFinishedJob());
    Assert.assertEquals(0, plumber2.getPersistCount());
  }

  @Test(timeout = 60_000L)
  public void testNormalStop() throws InterruptedException
  {
    final TestFirehose firehose = new TestFirehose(rows.iterator());
    final TestFirehoseV2 firehoseV2 = new TestFirehoseV2(rows.iterator());
    final RealtimeIOConfig ioConfig = new RealtimeIOConfig(
        new FirehoseFactory()
        {
          @Override
          public Firehose connect(InputRowParser parser, File temporaryDirectory)
          {
            return firehose;
          }
        },
        (schema, config, metrics) -> plumber,
        null
    );
    RealtimeIOConfig ioConfig2 = new RealtimeIOConfig(
        null,
        (schema, config, metrics) -> plumber2,
        (parser, arg) -> firehoseV2
    );

    final FireDepartment department_0 = new FireDepartment(schema3, ioConfig, tuningConfig_0);
    final FireDepartment department_1 = new FireDepartment(schema3, ioConfig2, tuningConfig_1);

    final RealtimeManager realtimeManager = new RealtimeManager(
        Arrays.asList(department_0, department_1),
        conglomerate,
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        null
    );

    realtimeManager.start();
    while (realtimeManager.getMetrics("testing").processed() < 2) {
      Thread.sleep(100);
    }
    realtimeManager.stop();

    Assert.assertTrue(firehose.isClosed());
    Assert.assertTrue(firehoseV2.isClosed());
    Assert.assertTrue(plumber.isFinishedJob());
    Assert.assertTrue(plumber2.isFinishedJob());
  }

  @Test(timeout = 60_000L)
  public void testStopByInterruption()
  {
    final SleepingFirehose firehose = new SleepingFirehose();
    final RealtimeIOConfig ioConfig = new RealtimeIOConfig(
        new FirehoseFactory()
        {
          @Override
          public Firehose connect(InputRowParser parser, File temporaryDirectory)
          {
            return firehose;
          }
        },
        (schema, config, metrics) -> plumber,
        null
    );

    final FireDepartment department_0 = new FireDepartment(schema, ioConfig, tuningConfig_0);

    final RealtimeManager realtimeManager = new RealtimeManager(
        Collections.singletonList(department_0),
        conglomerate,
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        null
    );

    realtimeManager.start();
    realtimeManager.stop();

    Assert.assertTrue(firehose.isClosed());
    Assert.assertFalse(plumber.isFinishedJob());
  }

  @Test(timeout = 60_000L)
  public void testQueryWithInterval() throws InterruptedException
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 270L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 236L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 316L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 240L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 5740L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 242L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 5800L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 156L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 238L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 2L, "idx", 294L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 2L, "idx", 224L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 2L, "idx", 332L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 2L, "idx", 226L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 6L, "idx", 4894L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 2L, "idx", 228L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 6L, "idx", 5010L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 2L, "idx", 194L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 2L, "idx", 252L)
    );

    realtimeManager3.start();

    awaitStarted();

    for (QueryRunner runner : QueryRunnerTestHelper.makeQueryRunners((GroupByQueryRunnerFactory) factory)) {
      GroupByQuery query = GroupByQuery
          .builder()
          .setDataSource(QueryRunnerTestHelper.dataSource)
          .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
          .setDimensions(new DefaultDimensionSpec("quality", "alias"))
          .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
          .setGranularity(QueryRunnerTestHelper.dayGran)
          .build();
      plumber.setRunners(ImmutableMap.of(query.getIntervals().get(0), runner));
      plumber2.setRunners(ImmutableMap.of(query.getIntervals().get(0), runner));

      Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(
          factory,
          realtimeManager3.getQueryRunnerForIntervals(
              query,
              QueryRunnerTestHelper.firstToThird.getIntervals()
          ),
          query
      );

      TestHelper.assertExpectedObjects(expectedResults, results, "interval");
    }

  }

  private void awaitStarted() throws InterruptedException
  {
    while (true) {
      boolean notAllStarted = realtimeManager3
          .getFireChiefs("testing").values().stream()
          .anyMatch(
              fireChief -> {
                final Plumber plumber = fireChief.getPlumber();
                return plumber == null || !((TestPlumber) plumber).isStartedJob();
              }
          );
      if (!notAllStarted) {
        break;
      }
      Thread.sleep(10);
    }
  }

  @Test(timeout = 60_000L)
  public void testQueryWithSegmentSpec() throws InterruptedException
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    realtimeManager3.start();

    awaitStarted();

    for (QueryRunner runner : QueryRunnerTestHelper.makeQueryRunners((GroupByQueryRunnerFactory) factory)) {
      GroupByQuery query = GroupByQuery
          .builder()
          .setDataSource(QueryRunnerTestHelper.dataSource)
          .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
          .setDimensions(new DefaultDimensionSpec("quality", "alias"))
          .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
          .setGranularity(QueryRunnerTestHelper.dayGran)
          .build();
      plumber.setRunners(ImmutableMap.of(query.getIntervals().get(0), runner));
      plumber2.setRunners(ImmutableMap.of(query.getIntervals().get(0), runner));

      Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(
          factory,
          realtimeManager3.getQueryRunnerForSegments(
              query,
              ImmutableList.of(
                  new SegmentDescriptor(
                      Intervals.of("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"),
                      "ver",
                      0
                  ))
          ),
          query
      );
      TestHelper.assertExpectedObjects(expectedResults, results, "segmentSpec");

      results = GroupByQueryRunnerTestHelper.runQuery(
          factory,
          realtimeManager3.getQueryRunnerForSegments(
              query,
              ImmutableList.of(
                  new SegmentDescriptor(
                      Intervals.of("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"),
                      "ver",
                      1
                  ))
          ),
          query
      );
      TestHelper.assertExpectedObjects(expectedResults, results, "segmentSpec");
    }

  }

  @Test(timeout = 60_000L)
  public void testQueryWithMultipleSegmentSpec() throws InterruptedException
  {

    List<Row> expectedResults_both_partitions = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-26", "alias", "business", "rows", 2L, "idx", 260L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-26", "alias", "health", "rows", 2L, "idx", 236L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-26", "alias", "mezzanine", "rows", 4L, "idx", 4556L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-26", "alias", "news", "rows", 2L, "idx", 284L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-26", "alias", "technology", "rows", 2L, "idx", 202L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-27", "alias", "automotive", "rows", 2L, "idx", 288L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-27", "alias", "entertainment", "rows", 2L, "idx", 326L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "automotive", "rows", 2L, "idx", 312L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "business", "rows", 2L, "idx", 248L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "entertainment", "rows", 2L, "idx", 326L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "health", "rows", 2L, "idx", 262L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "mezzanine", "rows", 6L, "idx", 5126L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "news", "rows", 2L, "idx", 254L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "premium", "rows", 6L, "idx", 5276L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "technology", "rows", 2L, "idx", 206L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "travel", "rows", 2L, "idx", 260L)
    );

    List<Row> expectedResults_single_partition_26_28 = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-26", "alias", "business", "rows", 1L, "idx", 130L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-26", "alias", "health", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-26", "alias", "mezzanine", "rows", 2L, "idx", 2278L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-26", "alias", "news", "rows", 1L, "idx", 142L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-26", "alias", "technology", "rows", 1L, "idx", 101L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-27", "alias", "automotive", "rows", 1L, "idx", 144L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-27", "alias", "entertainment", "rows", 1L, "idx", 163L)
    );

    List<Row> expectedResults_single_partition_28_29 = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "automotive", "rows", 1L, "idx", 156L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "business", "rows", 1L, "idx", 124L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "entertainment", "rows", 1L, "idx", 163L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "health", "rows", 1L, "idx", 131L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "mezzanine", "rows", 3L, "idx", 2563L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "news", "rows", 1L, "idx", 127L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "premium", "rows", 3L, "idx", 2638L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "technology", "rows", 1L, "idx", 103L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-28", "alias", "travel", "rows", 1L, "idx", 130L)
    );

    realtimeManager3.start();

    awaitStarted();

    final Interval interval_26_28 = Intervals.of("2011-03-26T00:00:00.000Z/2011-03-28T00:00:00.000Z");
    final Interval interval_28_29 = Intervals.of("2011-03-28T00:00:00.000Z/2011-03-29T00:00:00.000Z");
    final SegmentDescriptor descriptor_26_28_0 = new SegmentDescriptor(interval_26_28, "ver0", 0);
    final SegmentDescriptor descriptor_28_29_0 = new SegmentDescriptor(interval_28_29, "ver1", 0);
    final SegmentDescriptor descriptor_26_28_1 = new SegmentDescriptor(interval_26_28, "ver0", 1);
    final SegmentDescriptor descriptor_28_29_1 = new SegmentDescriptor(interval_28_29, "ver1", 1);

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(
            new MultipleSpecificSegmentSpec(
                ImmutableList.of(
                    descriptor_26_28_0,
                    descriptor_28_29_0,
                    descriptor_26_28_1,
                    descriptor_28_29_1
                )))
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final Map<Interval, QueryRunner> runnerMap = ImmutableMap.of(
        interval_26_28,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            "druid.sample.numeric.tsv.top",
            null
        ),
        interval_28_29,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            "druid.sample.numeric.tsv.bottom",
            null
        )
    );
    plumber.setRunners(runnerMap);
    plumber2.setRunners(runnerMap);

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        query.getQuerySegmentSpec().lookup(query, realtimeManager3),
        query
    );
    TestHelper.assertExpectedObjects(expectedResults_both_partitions, results, "multi-segmentSpec");

    results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        realtimeManager3.getQueryRunnerForSegments(
            query,
            ImmutableList.of(
                descriptor_26_28_0)
        ),
        query
    );
    TestHelper.assertExpectedObjects(expectedResults_single_partition_26_28, results, "multi-segmentSpec");

    results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        realtimeManager3.getQueryRunnerForSegments(
            query,
            ImmutableList.of(
                descriptor_28_29_0)
        ),
        query
    );
    TestHelper.assertExpectedObjects(expectedResults_single_partition_28_29, results, "multi-segmentSpec");

    results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        realtimeManager3.getQueryRunnerForSegments(
            query,
            ImmutableList.of(
                descriptor_26_28_1)
        ),
        query
    );
    TestHelper.assertExpectedObjects(expectedResults_single_partition_26_28, results, "multi-segmentSpec");

    results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        realtimeManager3.getQueryRunnerForSegments(
            query,
            ImmutableList.of(
                descriptor_28_29_1)
        ),
        query
    );
    TestHelper.assertExpectedObjects(expectedResults_single_partition_28_29, results, "multi-segmentSpec");

  }

  private static Pair<GroupByQueryRunnerFactory, Closer> initFactory()
  {
    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);
    return GroupByQueryRunnerTest.makeQueryRunnerFactory(config);
  }

  private static TestInputRowHolder makeRow(final long timestamp)
  {
    return new TestInputRowHolder(timestamp, null);
  }

  private static TestInputRowHolder makeRow(final RuntimeException e)
  {
    return new TestInputRowHolder(0, e);
  }

  private static class TestInputRowHolder
  {
    private long timestamp;
    private RuntimeException exception;

    public TestInputRowHolder(long timestamp, RuntimeException exception)
    {
      this.timestamp = timestamp;
      this.exception = exception;
    }

    public InputRow getRow()
    {
      if (exception != null) {
        throw exception;
      }

      return new InputRow()
      {
        @Override
        public List<String> getDimensions()
        {
          return Collections.singletonList("testDim");
        }

        @Override
        public long getTimestampFromEpoch()
        {
          return timestamp;
        }

        @Override
        public DateTime getTimestamp()
        {
          return DateTimes.utc(timestamp);
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
  }

  private static class TestFirehose implements Firehose
  {
    private final Iterator<TestInputRowHolder> rows;
    private boolean closed;

    private TestFirehose(Iterator<TestInputRowHolder> rows)
    {
      this.rows = rows;
    }

    @Override
    public boolean hasMore()
    {
      return rows.hasNext();
    }

    @Nullable
    @Override
    public InputRow nextRow()
    {
      final TestInputRowHolder holder = rows.next();
      if (holder == null) {
        return null;
      } else {
        return holder.getRow();
      }
    }

    @Override
    public Runnable commit()
    {
      return Runnables.getNoopRunnable();
    }

    public boolean isClosed()
    {
      return closed;
    }

    @Override
    public void close()
    {
      closed = true;
    }
  }

  private static class TestFirehoseV2 implements FirehoseV2
  {
    private final Iterator<TestInputRowHolder> rows;
    private InputRow currRow;
    private boolean stop;
    private boolean closed;

    private TestFirehoseV2(Iterator<TestInputRowHolder> rows)
    {
      this.rows = rows;
    }

    private void nextMessage()
    {
      currRow = null;
      while (currRow == null) {
        final TestInputRowHolder holder = rows.next();
        currRow = holder == null ? null : holder.getRow();
      }
    }

    @Override
    public void close()
    {
      closed = true;
    }

    public boolean isClosed()
    {
      return closed;
    }

    @Override
    public boolean advance()
    {
      stop = !rows.hasNext();
      if (stop) {
        return false;
      }

      nextMessage();
      return true;
    }

    @Override
    public InputRow currRow()
    {
      return currRow;
    }

    @Override
    public Committer makeCommitter()
    {
      return new Committer()
      {
        @Override
        public Object getMetadata()
        {
          return null;
        }

        @Override
        public void run()
        {
        }
      };
    }

    @Override
    public void start()
    {
      nextMessage();
    }
  }

  private static class SleepingFirehose implements Firehose
  {
    private boolean closed;

    @Override
    public boolean hasMore()
    {
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return true;
    }

    @Nullable
    @Override
    public InputRow nextRow()
    {
      return null;
    }

    @Override
    public Runnable commit()
    {
      return null;
    }

    public boolean isClosed()
    {
      return closed;
    }

    @Override
    public void close()
    {
      closed = true;
    }
  }

  private static class TestPlumber implements Plumber
  {
    private final Sink sink;


    private volatile boolean startedJob = false;
    private volatile boolean finishedJob = false;
    private volatile int persistCount = 0;

    private Map<Interval, QueryRunner> runners;

    private TestPlumber(Sink sink)
    {
      this.sink = sink;
    }

    private boolean isStartedJob()
    {
      return startedJob;
    }

    private boolean isFinishedJob()
    {
      return finishedJob;
    }

    private int getPersistCount()
    {
      return persistCount;
    }

    @Override
    public Object startJob()
    {
      startedJob = true;
      return null;
    }

    @Override
    public IncrementalIndexAddResult add(InputRow row, Supplier<Committer> committerSupplier)
        throws IndexSizeExceededException
    {
      if (row == null) {
        return Plumber.THROWAWAY;
      }

      Sink sink = getSink(row.getTimestampFromEpoch());

      if (sink == null) {
        return Plumber.THROWAWAY;
      }

      return sink.add(row, false);
    }

    public Sink getSink(long timestamp)
    {
      if (sink.getInterval().contains(timestamp)) {
        return sink;
      }
      return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> QueryRunner<T> getQueryRunner(final Query<T> query)
    {
      if (runners == null) {
        throw new UnsupportedOperationException();
      }

      final BaseQuery baseQuery = (BaseQuery) query;

      if (baseQuery.getQuerySegmentSpec() instanceof MultipleIntervalSegmentSpec) {
        return factory.getToolchest()
                      .mergeResults(
                          factory.mergeRunners(
                              Execs.directExecutor(),
                              Iterables.transform(
                                  baseQuery.getIntervals(),
                                  new Function<Interval, QueryRunner<T>>()
                                  {
                                    @Override
                                    public QueryRunner<T> apply(Interval input)
                                    {
                                      return runners.get(input);
                                    }
                                  }
                              )
                          )
                      );
      }

      Assert.assertEquals(1, query.getIntervals().size());

      final SegmentDescriptor descriptor =
          ((SpecificSegmentSpec) ((BaseQuery) query).getQuerySegmentSpec()).getDescriptor();

      return new SpecificSegmentQueryRunner<T>(
          runners.get(descriptor.getInterval()),
          new SpecificSegmentSpec(descriptor)
      );
    }

    @Override
    public void persist(Committer committer)
    {
      persistCount++;
    }

    @Override
    public void finishJob()
    {
      finishedJob = true;
    }

    public void setRunners(Map<Interval, QueryRunner> runners)
    {
      this.runners = runners;
    }
  }

}

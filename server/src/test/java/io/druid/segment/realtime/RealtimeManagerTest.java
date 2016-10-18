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

package io.druid.segment.realtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.FirehoseFactoryV2;
import io.druid.data.input.FirehoseV2;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Granularity;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.PlumberSchool;
import io.druid.segment.realtime.plumber.Sink;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.utils.Runnables;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 */
public class RealtimeManagerTest
{
  private static QueryRunnerFactory factory;

  private RealtimeManager realtimeManager;
  private RealtimeManager realtimeManager2;
  private RealtimeManager realtimeManager3;
  private DataSchema schema;
  private DataSchema schema2;
  private TestPlumber plumber;
  private TestPlumber plumber2;
  private CountDownLatch chiefStartedLatch;
  private RealtimeTuningConfig tuningConfig_0;
  private RealtimeTuningConfig tuningConfig_1;
  private DataSchema schema3;

  @BeforeClass
  public static void setupStatic()
  {
    factory = initFactory();
  }

  @Before
  public void setUp() throws Exception
  {
    final List<TestInputRowHolder> rows = Arrays.asList(
        makeRow(new DateTime("9000-01-01").getMillis()),
        makeRow(new ParseException("parse error")),
        null,
        makeRow(new DateTime().getMillis())
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();

    schema = new DataSchema(
        "test",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularity.HOUR, QueryGranularities.NONE, null),
        jsonMapper
    );
    schema2 = new DataSchema(
        "testV2",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularity.HOUR, QueryGranularities.NONE, null),
        jsonMapper
    );
    RealtimeIOConfig ioConfig = new RealtimeIOConfig(
        new FirehoseFactory()
        {
          @Override
          public Firehose connect(InputRowParser parser) throws IOException
          {
            return new TestFirehose(rows.iterator());
          }
        },
        new PlumberSchool()
        {
          @Override
          public Plumber findPlumber(
              DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics
          )
          {
            return plumber;
          }
        },
        null
    );
    RealtimeIOConfig ioConfig2 = new RealtimeIOConfig(
        null,
        new PlumberSchool()
        {
          @Override
          public Plumber findPlumber(
              DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics
          )
          {
            return plumber2;
          }
        },
        new FirehoseFactoryV2()
        {
          @Override
          public FirehoseV2 connect(InputRowParser parser, Object arg1) throws IOException, ParseException
          {
            return new TestFirehoseV2(rows.iterator());
          }
        }
    );
    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        1,
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
        null
    );
    plumber = new TestPlumber(new Sink(
        new Interval("0/P5000Y"),
        schema,
        tuningConfig.getShardSpec(),
        new DateTime().toString(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.isReportParseExceptions()
    ));

    realtimeManager = new RealtimeManager(
        Arrays.<FireDepartment>asList(
            new FireDepartment(
                schema,
                ioConfig,
                tuningConfig
            )
        ),
        null
    );
    plumber2 = new TestPlumber(new Sink(
        new Interval("0/P5000Y"),
        schema2,
        tuningConfig.getShardSpec(),
        new DateTime().toString(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.isReportParseExceptions()
    ));

    realtimeManager2 = new RealtimeManager(
        Arrays.<FireDepartment>asList(
            new FireDepartment(
                schema2,
                ioConfig2,
                tuningConfig
            )
        ),
        null
    );

    tuningConfig_0 = new RealtimeTuningConfig(
        1,
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
        null
    );

    tuningConfig_1 = new RealtimeTuningConfig(
        1,
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
        null
    );

    schema3 = new DataSchema(
        "testing",
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("ignore")},
        new UniformGranularitySpec(Granularity.HOUR, QueryGranularities.NONE, null),
        jsonMapper
    );

    FireDepartment department_0 = new FireDepartment(schema3, ioConfig, tuningConfig_0);
    FireDepartment department_1 = new FireDepartment(schema3, ioConfig2, tuningConfig_1);

    QueryRunnerFactoryConglomerate conglomerate = new QueryRunnerFactoryConglomerate()
    {
      @Override
      public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query)
      {
        return factory;
      }
    };

    chiefStartedLatch = new CountDownLatch(2);

    RealtimeManager.FireChief fireChief_0 = new RealtimeManager.FireChief(department_0, conglomerate)
    {
      @Override
      public void run()
      {
        super.initPlumber();
        chiefStartedLatch.countDown();
      }
    };

    RealtimeManager.FireChief fireChief_1 = new RealtimeManager.FireChief(department_1, conglomerate)
    {
      @Override
      public void run()
      {
        super.initPlumber();
        chiefStartedLatch.countDown();
      }
    };


    realtimeManager3 = new RealtimeManager(
        Arrays.asList(department_0, department_1),
        conglomerate,
        ImmutableMap.<String, Map<Integer, RealtimeManager.FireChief>>of(
            "testing",
            ImmutableMap.of(
                0,
                fireChief_0,
                1,
                fireChief_1
            )
        )
    );

    startFireChiefWithPartitionNum(fireChief_0, 0);
    startFireChiefWithPartitionNum(fireChief_1, 1);
  }

  private void startFireChiefWithPartitionNum(RealtimeManager.FireChief fireChief, int partitionNum)
  {
    fireChief.setName(
        String.format(
            "chief-%s[%s]",
            "testing",
            partitionNum
        )
    );
    fireChief.start();
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
    Assert.assertEquals(1, realtimeManager.getMetrics("test").thrownAway());
    Assert.assertEquals(2, realtimeManager.getMetrics("test").unparseable());
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

  @Test(timeout = 10_000L)
  public void testQueryWithInterval() throws IOException, InterruptedException
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

    chiefStartedLatch.await();

    for (QueryRunner runner : QueryRunnerTestHelper.makeQueryRunners((GroupByQueryRunnerFactory) factory)) {
      GroupByQuery query = GroupByQuery
          .builder()
          .setDataSource(QueryRunnerTestHelper.dataSource)
          .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
          .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
          .setAggregatorSpecs(
              Arrays.asList(
                  QueryRunnerTestHelper.rowsCount,
                  new LongSumAggregatorFactory("idx", "index")
              )
          )
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

      TestHelper.assertExpectedObjects(expectedResults, results, "");
    }

  }

  @Test(timeout = 10_000L)
  public void testQueryWithSegmentSpec() throws IOException, InterruptedException
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

    chiefStartedLatch.await();

    for (QueryRunner runner : QueryRunnerTestHelper.makeQueryRunners((GroupByQueryRunnerFactory) factory)) {
      GroupByQuery query = GroupByQuery
          .builder()
          .setDataSource(QueryRunnerTestHelper.dataSource)
          .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
          .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
          .setAggregatorSpecs(
              Arrays.asList(
                  QueryRunnerTestHelper.rowsCount,
                  new LongSumAggregatorFactory("idx", "index")
              )
          )
          .setGranularity(QueryRunnerTestHelper.dayGran)
          .build();
      plumber.setRunners(ImmutableMap.of(query.getIntervals().get(0), runner));
      plumber2.setRunners(ImmutableMap.of(query.getIntervals().get(0), runner));

      Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(
          factory,
          realtimeManager3.getQueryRunnerForSegments(
              query,
              ImmutableList.<SegmentDescriptor>of(
                  new SegmentDescriptor(
                      new Interval("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"),
                      "ver",
                      0
                  ))
          ),
          query
      );
      TestHelper.assertExpectedObjects(expectedResults, results, "");

      results = GroupByQueryRunnerTestHelper.runQuery(
          factory,
          realtimeManager3.getQueryRunnerForSegments(
              query,
              ImmutableList.<SegmentDescriptor>of(
                  new SegmentDescriptor(
                      new Interval("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"),
                      "ver",
                      1
                  ))
          ),
          query
      );
      TestHelper.assertExpectedObjects(expectedResults, results, "");
    }

  }

  @Test(timeout = 10_000L)
  public void testQueryWithMultipleSegmentSpec() throws IOException, InterruptedException
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

    chiefStartedLatch.await();

    final Interval interval_26_28 = new Interval("2011-03-26T00:00:00.000Z/2011-03-28T00:00:00.000Z");
    final Interval interval_28_29 = new Interval("2011-03-28T00:00:00.000Z/2011-03-29T00:00:00.000Z");
    final SegmentDescriptor descriptor_26_28_0 = new SegmentDescriptor(interval_26_28, "ver0", 0);
    final SegmentDescriptor descriptor_28_29_0 = new SegmentDescriptor(interval_28_29, "ver1", 0);
    final SegmentDescriptor descriptor_26_28_1 = new SegmentDescriptor(interval_26_28, "ver0", 1);
    final SegmentDescriptor descriptor_28_29_1 = new SegmentDescriptor(interval_28_29, "ver1", 1);

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(
            new MultipleSpecificSegmentSpec(
                ImmutableList.<SegmentDescriptor>of(
                    descriptor_26_28_0,
                    descriptor_28_29_0,
                    descriptor_26_28_1,
                    descriptor_28_29_1
                )))
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final Map<Interval, QueryRunner> runnerMap = ImmutableMap.<Interval, QueryRunner>of(
        interval_26_28,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            "druid.sample.tsv.top",
            null
        )
        ,
        interval_28_29,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            "druid.sample.tsv.bottom",
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
    TestHelper.assertExpectedObjects(expectedResults_both_partitions, results, "");

    results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        realtimeManager3.getQueryRunnerForSegments(
            query,
            ImmutableList.<SegmentDescriptor>of(
                descriptor_26_28_0)
        ),
        query
    );
    TestHelper.assertExpectedObjects(expectedResults_single_partition_26_28, results, "");

    results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        realtimeManager3.getQueryRunnerForSegments(
            query,
            ImmutableList.<SegmentDescriptor>of(
                descriptor_28_29_0)
        ),
        query
    );
    TestHelper.assertExpectedObjects(expectedResults_single_partition_28_29, results, "");

    results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        realtimeManager3.getQueryRunnerForSegments(
            query,
            ImmutableList.<SegmentDescriptor>of(
                descriptor_26_28_1)
        ),
        query
    );
    TestHelper.assertExpectedObjects(expectedResults_single_partition_26_28, results, "");

    results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        realtimeManager3.getQueryRunnerForSegments(
            query,
            ImmutableList.<SegmentDescriptor>of(
                descriptor_28_29_1)
        ),
        query
    );
    TestHelper.assertExpectedObjects(expectedResults_single_partition_28_29, results, "");

  }

  private static GroupByQueryRunnerFactory initFactory()
  {
    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);
    return GroupByQueryRunnerTest.makeQueryRunnerFactory(config);
  }

  @After
  public void tearDown() throws Exception
  {
    realtimeManager.stop();
    realtimeManager2.stop();
    realtimeManager3.stop();
  }

  private TestInputRowHolder makeRow(final long timestamp)
  {
    return new TestInputRowHolder(timestamp, null);
  }

  private TestInputRowHolder makeRow(final RuntimeException e)
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
          return Arrays.asList("testDim");
        }

        @Override
        public long getTimestampFromEpoch()
        {
          return timestamp;
        }

        @Override
        public DateTime getTimestamp()
        {
          return new DateTime(timestamp);
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

  private static class InfiniteTestFirehose implements Firehose
  {
    private boolean hasMore = true;

    @Override
    public boolean hasMore()
    {
      return hasMore;
    }

    @Override
    public InputRow nextRow()
    {
      return null;
    }

    @Override
    public Runnable commit()
    {
      return Runnables.getNoopRunnable();
    }

    @Override
    public void close() throws IOException
    {
      hasMore = false;
    }
  }

  private static class TestFirehose implements Firehose
  {
    private final Iterator<TestInputRowHolder> rows;

    private TestFirehose(Iterator<TestInputRowHolder> rows)
    {
      this.rows = rows;
    }

    @Override
    public boolean hasMore()
    {
      return rows.hasNext();
    }

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

    @Override
    public void close() throws IOException
    {
    }
  }

  private static class TestFirehoseV2 implements FirehoseV2
  {
    private final Iterator<TestInputRowHolder> rows;
    private InputRow currRow;
    private boolean stop;

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
    public void close() throws IOException
    {
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
    public void start() throws Exception
    {
      nextMessage();
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
    public int add(InputRow row, Supplier<Committer> committerSupplier) throws IndexSizeExceededException
    {
      if (row == null) {
        return -1;
      }

      Sink sink = getSink(row.getTimestampFromEpoch());

      if (sink == null) {
        return -1;
      }

      return sink.add(row);
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
                              MoreExecutors.sameThreadExecutor(),
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

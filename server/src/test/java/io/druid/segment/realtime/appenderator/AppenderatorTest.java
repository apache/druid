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

package io.druid.segment.realtime.appenderator;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.Druids;
import io.druid.query.QueryPlus;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AppenderatorTest
{
  private static final List<SegmentIdentifier> IDENTIFIERS = ImmutableList.of(
      SI("2000/2001", "A", 0),
      SI("2000/2001", "A", 1),
      SI("2001/2002", "A", 0)
  );

  @Test
  public void testSimpleIngestion() throws Exception
  {
    try (final AppenderatorTester tester = new AppenderatorTester(2, true)) {
      final Appenderator appenderator = tester.getAppenderator();
      boolean thrown;

      final ConcurrentMap<String, String> commitMetadata = new ConcurrentHashMap<>();
      final Supplier<Committer> committerSupplier = committerSupplierFromConcurrentMap(commitMetadata);

      // startJob
      Assert.assertEquals(null, appenderator.startJob());

      // getDataSource
      Assert.assertEquals(AppenderatorTester.DATASOURCE, appenderator.getDataSource());

      // add
      commitMetadata.put("x", "1");
      Assert.assertEquals(1,
                          appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier)
                                      .getNumRowsInSegment()
      );

      commitMetadata.put("x", "2");
      Assert.assertEquals(2,
                          appenderator.add(IDENTIFIERS.get(0), IR("2000", "bar", 2), committerSupplier)
                                      .getNumRowsInSegment()
      );

      commitMetadata.put("x", "3");
      Assert.assertEquals(1,
                          appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 4), committerSupplier)
                                      .getNumRowsInSegment()
      );

      // getSegments
      Assert.assertEquals(IDENTIFIERS.subList(0, 2), sorted(appenderator.getSegments()));

      // getRowCount
      Assert.assertEquals(2, appenderator.getRowCount(IDENTIFIERS.get(0)));
      Assert.assertEquals(1, appenderator.getRowCount(IDENTIFIERS.get(1)));
      thrown = false;
      try {
        appenderator.getRowCount(IDENTIFIERS.get(2));
      }
      catch (IllegalStateException e) {
        thrown = true;
      }
      Assert.assertTrue(thrown);

      // push all
      final SegmentsAndMetadata segmentsAndMetadata = appenderator.push(
          appenderator.getSegments(),
          committerSupplier.get(),
          false
      ).get();
      Assert.assertEquals(ImmutableMap.of("x", "3"), (Map<String, String>) segmentsAndMetadata.getCommitMetadata());
      Assert.assertEquals(
          IDENTIFIERS.subList(0, 2),
          sorted(
              Lists.transform(
                  segmentsAndMetadata.getSegments(),
                  new Function<DataSegment, SegmentIdentifier>()
                  {
                    @Override
                    public SegmentIdentifier apply(DataSegment input)
                    {
                      return SegmentIdentifier.fromDataSegment(input);
                    }
                  }
              )
          )
      );
      Assert.assertEquals(sorted(tester.getPushedSegments()), sorted(segmentsAndMetadata.getSegments()));

      // clear
      appenderator.clear();
      Assert.assertTrue(appenderator.getSegments().isEmpty());
    }
  }

  @Test
  public void testMaxRowsInMemory() throws Exception
  {
    try (final AppenderatorTester tester = new AppenderatorTester(3, true)) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = new Supplier<Committer>()
      {
        @Override
        public Committer get()
        {
          final Object metadata = ImmutableMap.of("eventCount", eventCount.get());

          return new Committer()
          {
            @Override
            public Object getMetadata()
            {
              return metadata;
            }

            @Override
            public void run()
            {
              // Do nothing
            }
          };
        }
      };

      Assert.assertEquals(0, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.startJob();
      Assert.assertEquals(0, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);
      Assert.assertEquals(1, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 1), committerSupplier);
      Assert.assertEquals(2, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 1), committerSupplier);
      Assert.assertEquals(2, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "baz", 1), committerSupplier);
      Assert.assertEquals(0, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 1), committerSupplier);
      Assert.assertEquals(1, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "bob", 1), committerSupplier);
      Assert.assertEquals(2, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.persist(ImmutableList.of(IDENTIFIERS.get(1)), committerSupplier.get());
      Assert.assertEquals(1, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.close();
      Assert.assertEquals(0, ((AppenderatorImpl) appenderator).getRowsInMemory());
    }
  }

  @Test
  public void testMaxRowsInMemoryDisallowIncrementalPersists() throws Exception
  {
    try (final AppenderatorTester tester = new AppenderatorTester(3, false)) {
      final Appenderator appenderator = tester.getAppenderator();
      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = () -> {
        final Object metadata = ImmutableMap.of("eventCount", eventCount.get());

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return metadata;
          }

          @Override
          public void run()
          {
            // Do nothing
          }
        };
      };

      Assert.assertEquals(0, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.startJob();
      Assert.assertEquals(0, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier, false);
      Assert.assertEquals(1, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 1), committerSupplier, false);
      Assert.assertEquals(2, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 1), committerSupplier, false);
      Assert.assertEquals(2, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "baz", 1), committerSupplier, false);
      Assert.assertEquals(3, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "qux", 1), committerSupplier, false);
      Assert.assertEquals(4, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "bob", 1), committerSupplier, false);
      Assert.assertEquals(5, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.persist(ImmutableList.of(IDENTIFIERS.get(1)), committerSupplier.get());
      Assert.assertEquals(3, ((AppenderatorImpl) appenderator).getRowsInMemory());
      appenderator.close();
      Assert.assertEquals(0, ((AppenderatorImpl) appenderator).getRowsInMemory());
    }
  }
  @Test
  public void testRestoreFromDisk() throws Exception
  {
    final RealtimeTuningConfig tuningConfig;
    try (final AppenderatorTester tester = new AppenderatorTester(2, true)) {
      final Appenderator appenderator = tester.getAppenderator();
      tuningConfig = tester.getTuningConfig();

      final AtomicInteger eventCount = new AtomicInteger(0);
      final Supplier<Committer> committerSupplier = new Supplier<Committer>()
      {
        @Override
        public Committer get()
        {
          final Object metadata = ImmutableMap.of("eventCount", eventCount.get());

          return new Committer()
          {
            @Override
            public Object getMetadata()
            {
              return metadata;
            }

            @Override
            public void run()
            {
              // Do nothing
            }
          };
        }
      };

      appenderator.startJob();
      eventCount.incrementAndGet();
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);
      eventCount.incrementAndGet();
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "bar", 2), committerSupplier);
      eventCount.incrementAndGet();
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "baz", 3), committerSupplier);
      eventCount.incrementAndGet();
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "qux", 4), committerSupplier);
      eventCount.incrementAndGet();
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "bob", 5), committerSupplier);
      appenderator.close();

      try (final AppenderatorTester tester2 = new AppenderatorTester(2, tuningConfig.getBasePersistDirectory(), true)) {
        final Appenderator appenderator2 = tester2.getAppenderator();
        Assert.assertEquals(ImmutableMap.of("eventCount", 4), appenderator2.startJob());
        Assert.assertEquals(ImmutableList.of(IDENTIFIERS.get(0)), appenderator2.getSegments());
        Assert.assertEquals(4, appenderator2.getRowCount(IDENTIFIERS.get(0)));
      }
    }
  }

  @Test(timeout = 10000L)
  public void testTotalRowCount() throws Exception
  {
    try (final AppenderatorTester tester = new AppenderatorTester(3, true)) {
      final Appenderator appenderator = tester.getAppenderator();
      final ConcurrentMap<String, String> commitMetadata = new ConcurrentHashMap<>();
      final Supplier<Committer> committerSupplier = committerSupplierFromConcurrentMap(commitMetadata);

      Assert.assertEquals(0, appenderator.getTotalRowCount());
      appenderator.startJob();
      Assert.assertEquals(0, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), committerSupplier);
      Assert.assertEquals(1, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "bar", 1), committerSupplier);
      Assert.assertEquals(2, appenderator.getTotalRowCount());

      appenderator.persistAll(committerSupplier.get()).get();
      Assert.assertEquals(2, appenderator.getTotalRowCount());
      appenderator.drop(IDENTIFIERS.get(0)).get();
      Assert.assertEquals(1, appenderator.getTotalRowCount());
      appenderator.drop(IDENTIFIERS.get(1)).get();
      Assert.assertEquals(0, appenderator.getTotalRowCount());

      appenderator.add(IDENTIFIERS.get(2), IR("2001", "bar", 1), committerSupplier);
      Assert.assertEquals(1, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(2), IR("2001", "baz", 1), committerSupplier);
      Assert.assertEquals(2, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(2), IR("2001", "qux", 1), committerSupplier);
      Assert.assertEquals(3, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(2), IR("2001", "bob", 1), committerSupplier);
      Assert.assertEquals(4, appenderator.getTotalRowCount());

      appenderator.persistAll(committerSupplier.get()).get();
      Assert.assertEquals(4, appenderator.getTotalRowCount());
      appenderator.drop(IDENTIFIERS.get(2)).get();
      Assert.assertEquals(0, appenderator.getTotalRowCount());

      appenderator.close();
      Assert.assertEquals(0, appenderator.getTotalRowCount());
    }
  }

  @Test
  public void testQueryByIntervals() throws Exception
  {
    try (final AppenderatorTester tester = new AppenderatorTester(2, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 2), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "foo", 4), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001", "foo", 8), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001T01", "foo", 16), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001T02", "foo", 32), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001T03", "foo", 64), Suppliers.ofInstance(Committers.nil()));

      // Query1: 2000/2001
      final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(AppenderatorTester.DATASOURCE)
                                           .intervals(ImmutableList.of(Intervals.of("2000/2001")))
                                           .aggregators(
                                               Arrays.<AggregatorFactory>asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .build();

      final List<Result<TimeseriesResultValue>> results1 =
          QueryPlus.wrap(query1).run(appenderator, ImmutableMap.of()).toList();
      Assert.assertEquals(
          "query1",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2000"),
                  new TimeseriesResultValue(ImmutableMap.<String, Object>of("count", 3L, "met", 7L))
              )
          ),
          results1
      );

      // Query2: 2000/2002
      final TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(AppenderatorTester.DATASOURCE)
                                           .intervals(ImmutableList.of(Intervals.of("2000/2002")))
                                           .aggregators(
                                               Arrays.<AggregatorFactory>asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .build();

      final List<Result<TimeseriesResultValue>> results2 =
          QueryPlus.wrap(query2).run(appenderator, ImmutableMap.of()).toList();
      Assert.assertEquals(
          "query2",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2000"),
                  new TimeseriesResultValue(ImmutableMap.<String, Object>of("count", 3L, "met", 7L))
              ),
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.<String, Object>of("count", 4L, "met", 120L))
              )
          ),
          results2
      );

      // Query3: 2000/2001T01
      final TimeseriesQuery query3 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(AppenderatorTester.DATASOURCE)
                                           .intervals(ImmutableList.of(Intervals.of("2000/2001T01")))
                                           .aggregators(
                                               Arrays.<AggregatorFactory>asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .build();

      final List<Result<TimeseriesResultValue>> results3 =
          QueryPlus.wrap(query3).run(appenderator, ImmutableMap.of()).toList();
      Assert.assertEquals(
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2000"),
                  new TimeseriesResultValue(ImmutableMap.<String, Object>of("count", 3L, "met", 7L))
              ),
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.<String, Object>of("count", 1L, "met", 8L))
              )
          ),
          results3
      );

      // Query4: 2000/2001T01, 2001T03/2001T04
      final TimeseriesQuery query4 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(AppenderatorTester.DATASOURCE)
                                           .intervals(
                                               ImmutableList.of(
                                                   Intervals.of("2000/2001T01"),
                                                   Intervals.of("2001T03/2001T04")
                                               )
                                           )
                                           .aggregators(
                                               Arrays.<AggregatorFactory>asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .build();

      final List<Result<TimeseriesResultValue>> results4 =
          QueryPlus.wrap(query4).run(appenderator, ImmutableMap.of()).toList();
      Assert.assertEquals(
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2000"),
                  new TimeseriesResultValue(ImmutableMap.<String, Object>of("count", 3L, "met", 7L))
              ),
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.<String, Object>of("count", 2L, "met", 72L))
              )
          ),
          results4
      );
    }
  }

  @Test
  public void testQueryBySegments() throws Exception
  {
    try (final AppenderatorTester tester = new AppenderatorTester(2, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 1), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(0), IR("2000", "foo", 2), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(1), IR("2000", "foo", 4), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001", "foo", 8), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001T01", "foo", 16), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001T02", "foo", 32), Suppliers.ofInstance(Committers.nil()));
      appenderator.add(IDENTIFIERS.get(2), IR("2001T03", "foo", 64), Suppliers.ofInstance(Committers.nil()));

      // Query1: segment #2
      final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(AppenderatorTester.DATASOURCE)
                                           .aggregators(
                                               Arrays.<AggregatorFactory>asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .intervals(
                                               new MultipleSpecificSegmentSpec(
                                                   ImmutableList.of(
                                                       new SegmentDescriptor(
                                                           IDENTIFIERS.get(2).getInterval(),
                                                           IDENTIFIERS.get(2).getVersion(),
                                                           IDENTIFIERS.get(2).getShardSpec().getPartitionNum()
                                                       )
                                                   )
                                               )
                                           )
                                           .build();

      final List<Result<TimeseriesResultValue>> results1 =
          QueryPlus.wrap(query1).run(appenderator, ImmutableMap.of()).toList();
      Assert.assertEquals(
          "query1",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.<String, Object>of("count", 4L, "met", 120L))
              )
          ),
          results1
      );

      // Query2: segment #2, partial
      final TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(AppenderatorTester.DATASOURCE)
                                           .aggregators(
                                               Arrays.<AggregatorFactory>asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .intervals(
                                               new MultipleSpecificSegmentSpec(
                                                   ImmutableList.of(
                                                       new SegmentDescriptor(
                                                           Intervals.of("2001/PT1H"),
                                                           IDENTIFIERS.get(2).getVersion(),
                                                           IDENTIFIERS.get(2).getShardSpec().getPartitionNum()
                                                       )
                                                   )
                                               )
                                           )
                                           .build();

      final List<Result<TimeseriesResultValue>> results2 =
          QueryPlus.wrap(query2).run(appenderator, ImmutableMap.of()).toList();
      Assert.assertEquals(
          "query2",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.<String, Object>of("count", 1L, "met", 8L))
              )
          ),
          results2
      );

      // Query3: segment #2, two disjoint intervals
      final TimeseriesQuery query3 = Druids.newTimeseriesQueryBuilder()
                                           .dataSource(AppenderatorTester.DATASOURCE)
                                           .aggregators(
                                               Arrays.<AggregatorFactory>asList(
                                                   new LongSumAggregatorFactory("count", "count"),
                                                   new LongSumAggregatorFactory("met", "met")
                                               )
                                           )
                                           .granularity(Granularities.DAY)
                                           .intervals(
                                               new MultipleSpecificSegmentSpec(
                                                   ImmutableList.of(
                                                       new SegmentDescriptor(
                                                           Intervals.of("2001/PT1H"),
                                                           IDENTIFIERS.get(2).getVersion(),
                                                           IDENTIFIERS.get(2).getShardSpec().getPartitionNum()
                                                       ),
                                                       new SegmentDescriptor(
                                                           Intervals.of("2001T03/PT1H"),
                                                           IDENTIFIERS.get(2).getVersion(),
                                                           IDENTIFIERS.get(2).getShardSpec().getPartitionNum()
                                                       )
                                                   )
                                               )
                                           )
                                           .build();

      final List<Result<TimeseriesResultValue>> results3 =
          QueryPlus.wrap(query3).run(appenderator, ImmutableMap.of()).toList();
      Assert.assertEquals(
          "query2",
          ImmutableList.of(
              new Result<>(
                  DateTimes.of("2001"),
                  new TimeseriesResultValue(ImmutableMap.<String, Object>of("count", 2L, "met", 72L))
              )
          ),
          results3
      );
    }
  }

  private static SegmentIdentifier SI(String interval, String version, int partitionNum)
  {
    return new SegmentIdentifier(
        AppenderatorTester.DATASOURCE,
        Intervals.of(interval),
        version,
        new LinearShardSpec(partitionNum)
    );
  }

  static InputRow IR(String ts, String dim, long met)
  {
    return new MapBasedInputRow(
        DateTimes.of(ts).getMillis(),
        ImmutableList.of("dim"),
        ImmutableMap.<String, Object>of(
            "dim",
            dim,
            "met",
            met
        )
    );
  }

  private static Supplier<Committer> committerSupplierFromConcurrentMap(final ConcurrentMap<String, String> map)
  {
    return new Supplier<Committer>()
    {
      @Override
      public Committer get()
      {
        final Map<String, String> mapCopy = ImmutableMap.copyOf(map);

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return mapCopy;
          }

          @Override
          public void run()
          {
            // Do nothing
          }
        };
      }
    };
  }

  private static <T> List<T> sorted(final List<T> xs)
  {
    final List<T> xsSorted = Lists.newArrayList(xs);
    Collections.sort(
        xsSorted, new Comparator<T>()
        {
          @Override
          public int compare(T a, T b)
          {
            if (a instanceof SegmentIdentifier && b instanceof SegmentIdentifier) {
              return ((SegmentIdentifier) a).getIdentifierAsString()
                                            .compareTo(((SegmentIdentifier) b).getIdentifierAsString());
            } else if (a instanceof DataSegment && b instanceof DataSegment) {
              return ((DataSegment) a).getIdentifier()
                                      .compareTo(((DataSegment) b).getIdentifier());
            } else {
              throw new IllegalStateException("WTF??");
            }
          }
        }
    );
    return xsSorted;
  }

}

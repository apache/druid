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

package org.apache.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.JavaScriptAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class QueryRunnerTestHelper
{

  public static final QueryWatcher NOOP_QUERYWATCHER = (query, future) -> {
  };

  public static final String dataSource = "testing";
  public static final Interval fullOnInterval = Intervals.of("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z");
  public static final SegmentId segmentId = SegmentId.of(dataSource, fullOnInterval, "dummy_version", 0);
  public static final UnionDataSource unionDataSource = new UnionDataSource(
      Stream.of(dataSource, dataSource, dataSource, dataSource).map(TableDataSource::new).collect(Collectors.toList())
  );

  public static final Granularity dayGran = Granularities.DAY;
  public static final Granularity allGran = Granularities.ALL;
  public static final Granularity monthGran = Granularities.MONTH;
  public static final String timeDimension = "__time";
  public static final String marketDimension = "market";
  public static final String qualityDimension = "quality";
  public static final String placementDimension = "placement";
  public static final String placementishDimension = "placementish";
  public static final String partialNullDimension = "partial_null_column";

  public static final List<String> dimensions = Lists.newArrayList(
      marketDimension,
      qualityDimension,
      placementDimension,
      placementishDimension
  );
  public static final String indexMetric = "index";
  public static final String uniqueMetric = "uniques";
  public static final String addRowsIndexConstantMetric = "addRowsIndexConstant";
  public static String dependentPostAggMetric = "dependentPostAgg";
  public static final CountAggregatorFactory rowsCount = new CountAggregatorFactory("rows");
  public static final LongSumAggregatorFactory indexLongSum = new LongSumAggregatorFactory("index", indexMetric);
  public static final LongSumAggregatorFactory __timeLongSum = new LongSumAggregatorFactory("sumtime", timeDimension);
  public static final DoubleSumAggregatorFactory indexDoubleSum = new DoubleSumAggregatorFactory("index", indexMetric);
  public static final String JS_COMBINE_A_PLUS_B = "function combine(a, b) { return a + b; }";
  public static final String JS_RESET_0 = "function reset() { return 0; }";
  public static final JavaScriptAggregatorFactory jsIndexSumIfPlacementishA = new JavaScriptAggregatorFactory(
      "nindex",
      Arrays.asList("placementish", "index"),
      "function aggregate(current, a, b) { if ((Array.isArray(a) && a.indexOf('a') > -1) || a === 'a') { return current + b; } else { return current; } }",
      JS_RESET_0,
      JS_COMBINE_A_PLUS_B,
      JavaScriptConfig.getEnabledInstance()
  );
  public static final JavaScriptAggregatorFactory jsCountIfTimeGreaterThan = new JavaScriptAggregatorFactory(
      "ntimestamps",
      Collections.singletonList("__time"),
      "function aggregate(current, t) { if (t > " +
      DateTimes.of("2011-04-01T12:00:00Z").getMillis() +
      ") { return current + 1; } else { return current; } }",
      JS_RESET_0,
      JS_COMBINE_A_PLUS_B,
      JavaScriptConfig.getEnabledInstance()
  );
  public static final JavaScriptAggregatorFactory jsPlacementishCount = new JavaScriptAggregatorFactory(
      "pishcount",
      Arrays.asList("placementish", "index"),
      "function aggregate(current, a) { if (Array.isArray(a)) { return current + a.length; } else if (typeof a === 'string') { return current + 1; } else { return current; } }",
      JS_RESET_0,
      JS_COMBINE_A_PLUS_B,
      JavaScriptConfig.getEnabledInstance()
  );
  public static final HyperUniquesAggregatorFactory qualityUniques = new HyperUniquesAggregatorFactory(
      "uniques",
      "quality_uniques"
  );
  public static final HyperUniquesAggregatorFactory qualityUniquesRounded = new HyperUniquesAggregatorFactory(
      "uniques",
      "quality_uniques",
      false,
      true
  );
  public static final CardinalityAggregatorFactory qualityCardinality = new CardinalityAggregatorFactory(
      "cardinality",
      Collections.singletonList(new DefaultDimensionSpec("quality", "quality")),
      false
  );
  public static final ConstantPostAggregator constant = new ConstantPostAggregator("const", 1L);
  public static final FieldAccessPostAggregator rowsPostAgg = new FieldAccessPostAggregator("rows", "rows");
  public static final FieldAccessPostAggregator indexPostAgg = new FieldAccessPostAggregator("index", "index");
  public static final ArithmeticPostAggregator addRowsIndexConstant = new ArithmeticPostAggregator(
      addRowsIndexConstantMetric,
      "+",
      Lists.newArrayList(constant, rowsPostAgg, indexPostAgg)
  );
  // dependent on AddRowsIndexContact postAgg
  public static final ArithmeticPostAggregator dependentPostAgg = new ArithmeticPostAggregator(
      dependentPostAggMetric,
      "+",
      Lists.newArrayList(
          constant,
          new FieldAccessPostAggregator(addRowsIndexConstantMetric, addRowsIndexConstantMetric),
          new FieldAccessPostAggregator("rows", "rows")
      )
  );

  public static final String hyperUniqueFinalizingPostAggMetric = "hyperUniqueFinalizingPostAggMetric";
  public static ArithmeticPostAggregator hyperUniqueFinalizingPostAgg = new ArithmeticPostAggregator(
      hyperUniqueFinalizingPostAggMetric,
      "+",
      Lists.newArrayList(
          new HyperUniqueFinalizingPostAggregator(uniqueMetric, uniqueMetric),
          new ConstantPostAggregator(null, 1)
      )
  );

  public static final List<AggregatorFactory> commonDoubleAggregators = Arrays.asList(
      rowsCount,
      indexDoubleSum,
      qualityUniques
  );

  public static final List<AggregatorFactory> commonFloatAggregators = Arrays.asList(
      new FloatSumAggregatorFactory("index", "indexFloat"),
      new CountAggregatorFactory("rows"),
      new HyperUniquesAggregatorFactory(
          "uniques",
          "quality_uniques"
      )
  );

  public static final double UNIQUES_9 = 9.019833517963864;
  public static final double UNIQUES_2 = 2.000977198748901d;
  public static final double UNIQUES_1 = 1.0002442201269182d;

  public static final String[] expectedFullOnIndexValues = new String[]{
      "4500.0", "6077.949111938477", "4922.488838195801", "5726.140853881836", "4698.468170166016",
      "4651.030891418457", "4398.145851135254", "4596.068244934082", "4434.630561828613", "0.0",
      "6162.801361083984", "5590.292701721191", "4994.298484802246", "5179.679672241211", "6288.556800842285",
      "6025.663551330566", "5772.855537414551", "5346.517524719238", "5497.331253051758", "5909.684387207031",
      "5862.711364746094", "5958.373008728027", "5224.882194519043", "5456.789611816406", "5456.095397949219",
      "4642.481948852539", "5023.572692871094", "5155.821723937988", "5350.3723220825195", "5236.997489929199",
      "4910.097717285156", "4507.608840942383", "4659.80500793457", "5354.878845214844", "4945.796455383301",
      "6459.080368041992", "4390.493583679199", "6545.758262634277", "6922.801231384277", "6023.452911376953",
      "6812.107475280762", "6368.713348388672", "6381.748748779297", "5631.245086669922", "4976.192253112793",
      "6541.463027954102", "5983.8513107299805", "5967.189498901367", "5567.139289855957", "4863.5944747924805",
      "4681.164360046387", "6122.321441650391", "5410.308860778809", "4846.676376342773", "5333.872688293457",
      "5013.053741455078", "4836.85563659668", "5264.486434936523", "4581.821243286133", "4680.233596801758",
      "4771.363662719727", "5038.354717254639", "4816.808464050293", "4684.095504760742", "5023.663467407227",
      "5889.72257232666", "4984.973915100098", "5664.220512390137", "5572.653915405273", "5537.123138427734",
      "5980.422874450684", "6243.834693908691", "5372.147285461426", "5690.728981018066", "5827.796455383301",
      "6141.0769119262695", "6082.3237228393555", "5678.771339416504", "6814.467971801758", "6626.151596069336",
      "5833.2095947265625", "4679.222328186035", "5367.9403076171875", "5410.445640563965", "5689.197135925293",
      "5240.5018310546875", "4790.912239074707", "4992.670921325684", "4796.888023376465", "5479.439590454102",
      "5506.567192077637", "4743.144546508789", "4913.282669067383", "4723.869743347168"
  };

  public static final String[] expectedFullOnIndexValuesDesc;

  static {
    List<String> list = new ArrayList<>(Arrays.asList(expectedFullOnIndexValues));
    Collections.reverse(list);
    expectedFullOnIndexValuesDesc = list.toArray(new String[0]);
  }

  public static final DateTime earliest = DateTimes.of("2011-01-12");
  public static final DateTime last = DateTimes.of("2011-04-15");

  public static final DateTime skippedDay = DateTimes.of("2011-01-21T00:00:00.000Z");

  public static final QuerySegmentSpec firstToThird = new MultipleIntervalSegmentSpec(
      Collections.singletonList(Intervals.of("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"))
  );
  public static final QuerySegmentSpec secondOnly = new MultipleIntervalSegmentSpec(
      Collections.singletonList(Intervals.of("2011-04-02T00:00:00.000Z/P1D"))
  );

  public static final QuerySegmentSpec fullOnIntervalSpec = new MultipleIntervalSegmentSpec(
      Collections.singletonList(fullOnInterval)
  );
  public static final QuerySegmentSpec emptyInterval = new MultipleIntervalSegmentSpec(
      Collections.singletonList(Intervals.of("2020-04-02T00:00:00.000Z/P1D"))
  );

  public static Iterable<Object[]> transformToConstructionFeeder(Iterable<?> in)
  {
    return Iterables.transform(in, (Function<Object, Object[]>) input -> new Object[]{input});
  }

  // simple cartesian iterable
  public static Iterable<Object[]> cartesian(final Iterable... iterables)
  {
    return new Iterable<Object[]>()
    {

      @Override
      public Iterator<Object[]> iterator()
      {
        return new Iterator<Object[]>()
        {
          private final Iterator[] iterators = new Iterator[iterables.length];
          private final Object[] cached = new Object[iterables.length];

          @Override
          public boolean hasNext()
          {
            return hasNext(0);
          }

          private boolean hasNext(int index)
          {
            if (iterators[index] == null) {
              iterators[index] = iterables[index].iterator();
            }
            for (; hasMore(index); cached[index] = null) {
              if (index == iterables.length - 1 || hasNext(index + 1)) {
                return true;
              }
            }
            iterators[index] = null;
            return false;
          }

          private boolean hasMore(int index)
          {
            if (cached[index] == null && iterators[index].hasNext()) {
              cached[index] = iterators[index].next();
            }
            return cached[index] != null;
          }

          @Override
          public Object[] next()
          {
            Object[] result = Arrays.copyOf(cached, cached.length);
            cached[cached.length - 1] = null;
            return result;
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException("remove");
          }
        };
      }
    };
  }

  /**
   * Check if a QueryRunner returned by {@link #makeQueryRunners(QueryRunnerFactory)} is vectorizable.
   */
  public static boolean isTestRunnerVectorizable(QueryRunner runner)
  {
    final String runnerName = runner.toString();
    return !("rtIndex".equals(runnerName) || "noRollupRtIndex".equals(runnerName));
  }

  public static <T, QueryType extends Query<T>> List<QueryRunner<T>> makeQueryRunners(
      QueryRunnerFactory<T, QueryType> factory
  )
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();
    final IncrementalIndex noRollupRtIndex = TestIndex.getNoRollupIncrementalTestIndex();
    final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    final QueryableIndex noRollupMMappedTestIndex = TestIndex.getNoRollupMMappedTestIndex();
    final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();
    return ImmutableList.of(
        makeQueryRunner(factory, new IncrementalIndexSegment(rtIndex, segmentId), ("rtIndex")),
        makeQueryRunner(factory, new IncrementalIndexSegment(noRollupRtIndex, segmentId), "noRollupRtIndex"),
        makeQueryRunner(factory, new QueryableIndexSegment(mMappedTestIndex, segmentId), "mMappedTestIndex"),
        makeQueryRunner(
            factory,
            new QueryableIndexSegment(noRollupMMappedTestIndex, segmentId),
            "noRollupMMappedTestIndex"
        ),
        makeQueryRunner(factory, new QueryableIndexSegment(mergedRealtimeIndex, segmentId), "mergedRealtimeIndex")
    );
  }

  @SuppressWarnings("unchecked")
  public static Collection<?> makeUnionQueryRunners(QueryRunnerFactory factory)
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();
    final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();

    return Arrays.asList(
        makeUnionQueryRunner(factory, new IncrementalIndexSegment(rtIndex, segmentId), "rtIndex"),
        makeUnionQueryRunner(factory, new QueryableIndexSegment(mMappedTestIndex, segmentId), "mMappedTestIndex"),
        makeUnionQueryRunner(
            factory,
            new QueryableIndexSegment(mergedRealtimeIndex, segmentId),
            "mergedRealtimeIndex"
        )
    );
  }

  public static <T, QueryType extends Query<T>> QueryRunner<T> makeQueryRunner(
      QueryRunnerFactory<T, QueryType> factory,
      String resourceFileName,
      final String runnerName
  )
  {
    return makeQueryRunner(
        factory,
        segmentId,
        new IncrementalIndexSegment(TestIndex.makeRealtimeIndex(resourceFileName), segmentId),
        runnerName
    );
  }

  public static <T, QueryType extends Query<T>> QueryRunner<T> makeQueryRunner(
      QueryRunnerFactory<T, QueryType> factory,
      Segment adapter,
      final String runnerName
  )
  {
    return makeQueryRunner(factory, segmentId, adapter, runnerName);
  }

  public static <T, QueryType extends Query<T>> QueryRunner<T> makeQueryRunner(
      QueryRunnerFactory<T, QueryType> factory,
      SegmentId segmentId,
      Segment adapter,
      final String runnerName
  )
  {
    return new FinalizeResultsQueryRunner<T>(
        new BySegmentQueryRunner<>(segmentId, adapter.getDataInterval().getStart(), factory.createRunner(adapter)),
        (QueryToolChest<T, Query<T>>) factory.getToolchest()
    )
    {
      @Override
      public String toString()
      {
        return runnerName;
      }
    };
  }

  public static <T> QueryRunner<T> makeUnionQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      Segment adapter,
      final String runnerName
  )
  {
    BySegmentQueryRunner<T> bySegmentQueryRunner =
        new BySegmentQueryRunner<>(segmentId, adapter.getDataInterval().getStart(), factory.createRunner(adapter));
    final QueryRunner<T> runner = new FluentQueryRunnerBuilder<T>(factory.getToolchest())
        .create(new UnionQueryRunner<>(bySegmentQueryRunner))
        .mergeResults()
        .applyPostMergeDecoration();

    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
      {
        return runner.run(queryPlus, responseContext);
      }

      @Override
      public String toString()
      {
        return runnerName;
      }
    };
  }

  public static <T> QueryRunner<T> makeFilteringQueryRunner(
      final VersionedIntervalTimeline<String, Segment> timeline,
      final QueryRunnerFactory<T, Query<T>> factory
  )
  {
    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    return new FluentQueryRunnerBuilder<T>(toolChest)
        .create(
            new QueryRunner<T>()
            {
              @Override
              public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
              {
                Query<T> query = queryPlus.getQuery();
                List<TimelineObjectHolder> segments = new ArrayList<>();
                for (Interval interval : query.getIntervals()) {
                  segments.addAll(timeline.lookup(interval));
                }
                List<Sequence<T>> sequences = new ArrayList<>();
                for (TimelineObjectHolder<String, Segment> holder : toolChest.filterSegments(query, segments)) {
                  Segment segment = holder.getObject().getChunk(0).getObject();
                  QueryPlus queryPlusRunning = queryPlus.withQuerySegmentSpec(
                      new SpecificSegmentSpec(
                          new SegmentDescriptor(
                              holder.getInterval(),
                              holder.getVersion(),
                              0
                          )
                      )
                  );
                  sequences.add(factory.createRunner(segment).run(queryPlusRunning, responseContext));
                }
                return new MergeSequence<>(query.getResultOrdering(), Sequences.simple(sequences));
              }
            }
        )
        .applyPreMergeDecoration()
        .mergeResults()
        .applyPostMergeDecoration();
  }

  public static IntervalChunkingQueryRunnerDecorator noopIntervalChunkingQueryRunnerDecorator()
  {
    return new IntervalChunkingQueryRunnerDecorator(null, null, null)
    {
      @Override
      public <T> QueryRunner<T> decorate(
          final QueryRunner<T> delegate,
          QueryToolChest<T, ? extends Query<T>> toolChest
      )
      {
        return new QueryRunner<T>()
        {
          @Override
          public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
          {
            return delegate.run(queryPlus, responseContext);
          }
        };
      }
    };
  }

  public static IntervalChunkingQueryRunnerDecorator sameThreadIntervalChunkingQueryRunnerDecorator()
  {
    return new IntervalChunkingQueryRunnerDecorator(
        Execs.directExecutor(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        new ServiceEmitter("dummy", "dummy", new NoopEmitter())
    );
  }

  public static Map<String, Object> of(Object... keyvalues)
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < keyvalues.length; i += 2) {
      builder.put(String.valueOf(keyvalues[i]), keyvalues[i + 1]);
    }
    return builder.build();
  }

  public static TimeseriesQueryRunnerFactory newTimeseriesQueryRunnerFactory()
  {
    return new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(noopIntervalChunkingQueryRunnerDecorator()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
  }

  public static Map<String, Object> orderedMap(Object... keyValues)
  {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put(keyValues[i].toString(), keyValues[i + 1]);
    }
    return map;
  }
}

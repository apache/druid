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

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.emitter.core.NoopEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.MergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.js.JavaScriptConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FloatSumAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class QueryRunnerTestHelper
{

  public static final QueryWatcher NOOP_QUERYWATCHER = (query, future) -> {};

  public static final String segmentId = "testSegment";
  public static final String dataSource = "testing";
  public static final UnionDataSource unionDataSource = new UnionDataSource(
      Lists.transform(
          Lists.newArrayList(dataSource, dataSource, dataSource, dataSource), new Function<String, TableDataSource>()
          {
            @Nullable
            @Override
            public TableDataSource apply(@Nullable String input)
            {
              return new TableDataSource(input);
            }
          }
      )
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
      Arrays.asList("__time"),
      "function aggregate(current, t) { if (t > " +
      new DateTime("2011-04-01T12:00:00Z").getMillis() +
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
  public static final CardinalityAggregatorFactory qualityCardinality = new CardinalityAggregatorFactory(
      "cardinality",
      Arrays.<DimensionSpec>asList(new DefaultDimensionSpec("quality", "quality")),
      false
  );
  public static final ConstantPostAggregator constant = new ConstantPostAggregator("const", 1L);
  public static final FieldAccessPostAggregator rowsPostAgg = new FieldAccessPostAggregator("rows", "rows");
  public static final FieldAccessPostAggregator indexPostAgg = new FieldAccessPostAggregator("index", "index");
  public static final ArithmeticPostAggregator addRowsIndexConstant =
      new ArithmeticPostAggregator(
          addRowsIndexConstantMetric, "+", Lists.newArrayList(constant, rowsPostAgg, indexPostAgg)
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

  public final static List<AggregatorFactory> commonFloatAggregators = Arrays.asList(
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
    List<String> list = new ArrayList(Arrays.asList(expectedFullOnIndexValues));
    Collections.reverse(list);
    expectedFullOnIndexValuesDesc = list.toArray(new String[list.size()]);
  }

  public static final DateTime earliest = new DateTime("2011-01-12");
  public static final DateTime last = new DateTime("2011-04-15");

  public static final DateTime skippedDay = new DateTime("2011-01-21T00:00:00.000Z");

  public static final QuerySegmentSpec firstToThird = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"))
  );
  public static final QuerySegmentSpec secondOnly = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("2011-04-02T00:00:00.000Z/P1D"))
  );
  public static final QuerySegmentSpec fullOnInterval = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z"))
  );
  public static final QuerySegmentSpec emptyInterval = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("2020-04-02T00:00:00.000Z/P1D"))
  );

  public static Iterable<Object[]> transformToConstructionFeeder(Iterable<?> in)
  {
    return Iterables.transform(
        in, new Function<Object, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(@Nullable Object input)
          {
            return new Object[]{input};
          }
        }
    );
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

  public static <T, QueryType extends Query<T>> List<QueryRunner<T>> makeQueryRunners(
      QueryRunnerFactory<T, QueryType> factory
  )
      throws IOException
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();
    final IncrementalIndex noRollupRtIndex = TestIndex.getNoRollupIncrementalTestIndex();
    final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    final QueryableIndex noRollupMMappedTestIndex = TestIndex.getNoRollupMMappedTestIndex();
    final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();
    return ImmutableList.of(
        makeQueryRunner(factory, new IncrementalIndexSegment(rtIndex, segmentId), "rtIndex"),
        makeQueryRunner(factory, new IncrementalIndexSegment(noRollupRtIndex, segmentId), "noRollupRtIndex"),
        makeQueryRunner(factory, new QueryableIndexSegment(segmentId, mMappedTestIndex), "mMappedTestIndex"),
        makeQueryRunner(
            factory,
            new QueryableIndexSegment(segmentId, noRollupMMappedTestIndex),
            "noRollupMMappedTestIndex"
        ),
        makeQueryRunner(factory, new QueryableIndexSegment(segmentId, mergedRealtimeIndex), "mergedRealtimeIndex")
    );
  }

  @SuppressWarnings("unchecked")
  public static Collection<?> makeUnionQueryRunners(
      QueryRunnerFactory factory,
      DataSource unionDataSource
  )
      throws IOException
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();
    final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();

    return Arrays.asList(
        makeUnionQueryRunner(factory, new IncrementalIndexSegment(rtIndex, segmentId), "rtIndex"),
        makeUnionQueryRunner(factory, new QueryableIndexSegment(segmentId, mMappedTestIndex), "mMappedTestIndex"),
        makeUnionQueryRunner(
            factory,
            new QueryableIndexSegment(segmentId, mergedRealtimeIndex),
            "mergedRealtimeIndex"
        )
    );
  }

  /**
   * Iterate through the iterables in a synchronous manner and return each step as an Object[]
   *
   * @param in The iterables to step through. (effectively columns)
   *
   * @return An iterable of Object[] containing the "rows" of the input (effectively rows)
   */
  public static Iterable<Object[]> transformToConstructionFeeder(Iterable<?>... in)
  {
    if (in == null) {
      return ImmutableList.<Object[]>of();
    }
    final List<Iterable<?>> iterables = Arrays.asList(in);
    final int length = in.length;
    final List<Iterator<?>> iterators = new ArrayList<>(in.length);
    for (Iterable<?> iterable : iterables) {
      iterators.add(iterable.iterator());
    }
    return new Iterable<Object[]>()
    {
      @Override
      public Iterator<Object[]> iterator()
      {
        return new Iterator<Object[]>()
        {
          @Override
          public boolean hasNext()
          {
            int hasMore = 0;
            for (Iterator<?> it : iterators) {
              if (it.hasNext()) {
                ++hasMore;
              }
            }
            return hasMore == length;
          }

          @Override
          public Object[] next()
          {
            final ArrayList<Object> list = new ArrayList<Object>(length);
            for (Iterator<?> it : iterators) {
              list.add(it.next());
            }
            return list.toArray();
          }

          @Override
          public void remove()
          {
            throw new UOE("Remove not supported");
          }
        };
      }
    };
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
      String segmentId,
      Segment adapter,
      final String runnerName
  )
  {
    return new FinalizeResultsQueryRunner<T>(
        new BySegmentQueryRunner<T>(
            segmentId, adapter.getDataInterval().getStart(),
            factory.createRunner(adapter)
        ),
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
    final QueryRunner<T> qr = new FluentQueryRunnerBuilder<T>(factory.getToolchest())
        .create(
            new UnionQueryRunner<T>(
                new BySegmentQueryRunner<T>(
                    segmentId, adapter.getDataInterval().getStart(),
                    factory.createRunner(adapter)
                )
            )
        )
        .mergeResults()
        .applyPostMergeDecoration();

    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(QueryPlus<T> queryPlus, Map<String, Object> responseContext)
      {
        return qr.run(queryPlus, responseContext);
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
              public Sequence<T> run(QueryPlus<T> queryPlus, Map<String, Object> responseContext)
              {
                Query<T> query = queryPlus.getQuery();
                List<TimelineObjectHolder> segments = Lists.newArrayList();
                for (Interval interval : query.getIntervals()) {
                  segments.addAll(timeline.lookup(interval));
                }
                List<Sequence<T>> sequences = Lists.newArrayList();
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

  public static IntervalChunkingQueryRunnerDecorator NoopIntervalChunkingQueryRunnerDecorator()
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
          public Sequence<T> run(QueryPlus<T> queryPlus, Map<String, Object> responseContext)
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
        MoreExecutors.sameThreadExecutor(),
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

  public static QueryRunnerFactoryConglomerate newConglomerate()
  {
    return new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
            .put(TimeseriesQuery.class, newTimeseriesQueryRunnerFactory())
            .build()
    );
  }

  public static TimeseriesQueryRunnerFactory newTimeseriesQueryRunnerFactory()
  {
    return new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(NoopIntervalChunkingQueryRunnerDecorator()),
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

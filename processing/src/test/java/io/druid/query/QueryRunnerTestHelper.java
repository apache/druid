/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.guava.Sequence;

import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 */
public class QueryRunnerTestHelper
{

  public static final QueryWatcher NOOP_QUERYWATCHER = new QueryWatcher()
  {
    @Override
    public void registerQuery(Query query, ListenableFuture future)
    {

    }
  };

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
  public static final QueryGranularity dayGran = QueryGranularity.DAY;
  public static final QueryGranularity allGran = QueryGranularity.ALL;
  public static final String marketDimension = "market";
  public static final String qualityDimension = "quality";
  public static final String placementDimension = "placement";
  public static final String placementishDimension = "placementish";
  public static final String indexMetric = "index";
  public static final String uniqueMetric = "uniques";
  public static final String addRowsIndexConstantMetric = "addRowsIndexConstant";
  public static String dependentPostAggMetric = "dependentPostAgg";
  public static final CountAggregatorFactory rowsCount = new CountAggregatorFactory("rows");
  public static final LongSumAggregatorFactory indexLongSum = new LongSumAggregatorFactory("index", "index");
  public static final LongSumAggregatorFactory __timeLongSum = new LongSumAggregatorFactory("sumtime", "__time");
  public static final DoubleSumAggregatorFactory indexDoubleSum = new DoubleSumAggregatorFactory("index", "index");
  public static final String JS_COMBINE_A_PLUS_B = "function combine(a, b) { return a + b; }";
  public static final String JS_RESET_0 = "function reset() { return 0; }";
  public static final JavaScriptAggregatorFactory jsIndexSumIfPlacementishA = new JavaScriptAggregatorFactory(
      "nindex",
      Arrays.asList("placementish", "index"),
      "function aggregate(current, a, b) { if ((Array.isArray(a) && a.indexOf('a') > -1) || a === 'a') { return current + b; } else { return current; } }",
      JS_RESET_0,
      JS_COMBINE_A_PLUS_B
  );
  public static final JavaScriptAggregatorFactory jsCountIfTimeGreaterThan = new JavaScriptAggregatorFactory(
      "ntimestamps",
      Arrays.asList("__time"),
      "function aggregate(current, t) { if (t > " +
      new DateTime("2011-04-01T12:00:00Z").getMillis() +
      ") { return current + 1; } else { return current; } }",
      JS_RESET_0,
      JS_COMBINE_A_PLUS_B
  );
  public static final JavaScriptAggregatorFactory jsPlacementishCount = new JavaScriptAggregatorFactory(
      "pishcount",
      Arrays.asList("placementish", "index"),
      "function aggregate(current, a) { if (Array.isArray(a)) { return current + a.length; } else if (typeof a === 'string') { return current + 1; } else { return current; } }",
      JS_RESET_0,
      JS_COMBINE_A_PLUS_B
  );
  public static final HyperUniquesAggregatorFactory qualityUniques = new HyperUniquesAggregatorFactory(
      "uniques",
      "quality_uniques"
  );
  public static final CardinalityAggregatorFactory qualityCardinality = new CardinalityAggregatorFactory(
      "cardinality",
      Arrays.asList("quality"),
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
      Lists.newArrayList(new HyperUniqueFinalizingPostAggregator(uniqueMetric), new ConstantPostAggregator(null, 1))
  );

  public static final List<AggregatorFactory> commonAggregators = Arrays.asList(
      rowsCount,
      indexDoubleSum,
      qualityUniques
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

  @SuppressWarnings("unchecked")
  public static Collection<?> makeQueryRunners(
      QueryRunnerFactory factory
  )
      throws IOException
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex(false);
    final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();
    final IncrementalIndex rtIndexOffheap = TestIndex.getIncrementalTestIndex(true);
    return Arrays.asList(
        new Object[][]{
            {
                makeQueryRunner(factory, new IncrementalIndexSegment(rtIndex, segmentId))
            },
            {
                makeQueryRunner(factory, new QueryableIndexSegment(segmentId, mMappedTestIndex))
            },
            {
                makeQueryRunner(factory, new QueryableIndexSegment(segmentId, mergedRealtimeIndex))
            },
            {
                makeQueryRunner(factory, new IncrementalIndexSegment(rtIndexOffheap, segmentId))
            }
        }
    );
  }

  @SuppressWarnings("unchecked")
  public static Collection<?> makeUnionQueryRunners(
      QueryRunnerFactory factory,
      DataSource unionDataSource
  )
      throws IOException
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex(false);
    final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();
    final IncrementalIndex rtIndexOffheap = TestIndex.getIncrementalTestIndex(true);

    return Arrays.asList(
        new Object[][]{
            {
                makeUnionQueryRunner(factory, new IncrementalIndexSegment(rtIndex, segmentId), unionDataSource)
            },
            {
                makeUnionQueryRunner(factory, new QueryableIndexSegment(segmentId, mMappedTestIndex), unionDataSource)
            },
            {
                makeUnionQueryRunner(
                    factory,
                    new QueryableIndexSegment(segmentId, mergedRealtimeIndex),
                    unionDataSource
                )
            },
            {
                makeUnionQueryRunner(factory, new IncrementalIndexSegment(rtIndexOffheap, segmentId), unionDataSource)
            }
        }
    );
  }

  public static <T> QueryRunner<T> makeQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      Segment adapter
  )
  {
    return new FinalizeResultsQueryRunner<T>(
        new BySegmentQueryRunner<T>(
            segmentId, adapter.getDataInterval().getStart(),
            factory.createRunner(adapter)
        ),
        factory.getToolchest()
    );
  }

  public static <T> QueryRunner<T> makeUnionQueryRunner(
      final QueryRunnerFactory<T, Query<T>> factory,
      final Segment adapter,
      final DataSource unionDataSource
  )
  {
    return new FinalizeResultsQueryRunner<T>(
        factory.getToolchest().postMergeQueryDecoration(
            factory.getToolchest().mergeResults(
                new UnionQueryRunner<T>(
                    Iterables.transform(
                        unionDataSource.getNames(), new Function<String, QueryRunner>()
                        {
                          @Nullable
                          @Override
                          public QueryRunner apply(@Nullable String input)
                          {
                            return new BySegmentQueryRunner<T>(
                                segmentId, adapter.getDataInterval().getStart(),
                                factory.createRunner(adapter)
                            );
                          }
                        }
                    ),
                    factory.getToolchest()
                )
            )
        ),
        factory.getToolchest()
    );
  }

  public static IntervalChunkingQueryRunnerDecorator NoopIntervalChunkingQueryRunnerDecorator()
  {
    return new IntervalChunkingQueryRunnerDecorator(null, null, null) {
      @Override
      public <T> QueryRunner<T> decorate(final QueryRunner<T> delegate,
          QueryToolChest<T, ? extends Query<T>> toolChest) {
        return new QueryRunner<T>() {
          @Override
          public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
          {
            return delegate.run(query, responseContext);
          }
        };
      }
    };
  }
}

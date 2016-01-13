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

package io.druid.query.groupby;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.guice.annotations.Global;
import io.druid.query.BaseQuery;
import io.druid.query.CacheStrategy;
import io.druid.query.DataSource;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.SubqueryQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class GroupByQueryQueryToolChest extends QueryToolChest<Row, GroupByQuery>
{
  private static final byte GROUPBY_QUERY = 0x14;
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>()
  {
  };
  private static final String GROUP_BY_MERGE_KEY = "groupByMerge";

  private final Supplier<GroupByQueryConfig> configSupplier;

  private final StupidPool<ByteBuffer> bufferPool;
  private final ObjectMapper jsonMapper;
  private GroupByQueryEngine engine; // For running the outer query around a subquery

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public GroupByQueryQueryToolChest(
      Supplier<GroupByQueryConfig> configSupplier,
      ObjectMapper jsonMapper,
      GroupByQueryEngine engine,
      @Global StupidPool<ByteBuffer> bufferPool,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.configSupplier = configSupplier;
    this.jsonMapper = jsonMapper;
    this.engine = engine;
    this.bufferPool = bufferPool;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Row> mergeResults(final QueryRunner<Row> runner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        if (BaseQuery.getContextBySegment(query, false)) {
          return runner.run(query, responseContext);
        }

        if (Boolean.valueOf(query.getContextValue(GROUP_BY_MERGE_KEY, "true"))) {
          return mergeGroupByResults(
              (GroupByQuery) query,
              runner,
              responseContext
          );
        }
        return runner.run(query, responseContext);
      }
    };
  }

  private Sequence<Row> mergeGroupByResults(
      final GroupByQuery query,
      QueryRunner<Row> runner,
      Map<String, Object> context
  )
  {
    // If there's a subquery, merge subquery results and then apply the aggregator

    final DataSource dataSource = query.getDataSource();

    if (dataSource instanceof QueryDataSource) {
      GroupByQuery subquery;
      try {
        subquery = (GroupByQuery) ((QueryDataSource) dataSource).getQuery().withOverriddenContext(query.getContext());
      }
      catch (ClassCastException e) {
        throw new UnsupportedOperationException("Subqueries must be of type 'group by'");
      }

      final Sequence<Row> subqueryResult = mergeGroupByResults(subquery, runner, context);
      final Set<AggregatorFactory> aggs = Sets.newHashSet();

      // Nested group-bys work by first running the inner query and then materializing the results in an incremental
      // index which the outer query is then run against. To build the incremental index, we use the fieldNames from
      // the aggregators for the outer query to define the column names so that the index will match the query. If
      // there are multiple types of aggregators in the outer query referencing the same fieldName, we will try to build
      // multiple columns of the same name using different aggregator types and will fail. Here, we permit multiple
      // aggregators of the same type referencing the same fieldName (and skip creating identical columns for the
      // subsequent ones) and return an error if the aggregator types are different.
      for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
        for (final AggregatorFactory transferAgg : aggregatorFactory.getRequiredColumns()) {
          if (Iterables.any(aggs, new Predicate<AggregatorFactory>()
          {
            @Override
            public boolean apply(AggregatorFactory agg)
            {
              return agg.getName().equals(transferAgg.getName()) && !agg.equals(transferAgg);
            }
          })) {
            throw new IAE("Inner aggregator can currently only be referenced by a single type of outer aggregator" +
                          " for '%s'", transferAgg.getName());
          }

          aggs.add(transferAgg);
        }
      }

      // We need the inner incremental index to have all the columns required by the outer query
      final GroupByQuery innerQuery = new GroupByQuery.Builder(subquery)
          .setAggregatorSpecs(Lists.newArrayList(aggs))
          .setInterval(subquery.getIntervals())
          .setPostAggregatorSpecs(Lists.<PostAggregator>newArrayList())
          .build();

      final GroupByQuery outerQuery = new GroupByQuery.Builder(query)
          .setLimitSpec(query.getLimitSpec().merge(subquery.getLimitSpec()))
          .build();
      final IncrementalIndex index = makeIncrementalIndex(innerQuery, subqueryResult);

      //Outer query might have multiple intervals, but they are expected to be non-overlapping and sorted which
      //is ensured by QuerySegmentSpec.
      //GroupByQueryEngine can only process one interval at a time, so we need to call it once per interval
      //and concatenate the results.
      return new ResourceClosingSequence<>(
          outerQuery.applyLimit(
              Sequences.concat(
                  Sequences.map(
                      Sequences.simple(outerQuery.getIntervals()),
                      new Function<Interval, Sequence<Row>>()
                      {
                        @Override
                        public Sequence<Row> apply(Interval interval)
                        {
                          return engine.process(
                              outerQuery.withQuerySegmentSpec(
                                  new MultipleIntervalSegmentSpec(ImmutableList.of(interval))
                              ),
                              new IncrementalIndexStorageAdapter(index)
                          );
                        }
                      }
                  )
              )
          ),
          index
      );
    } else {
      final IncrementalIndex index = makeIncrementalIndex(
          query, runner.run(
              new GroupByQuery(
                  query.getDataSource(),
                  query.getQuerySegmentSpec(),
                  query.getDimFilter(),
                  query.getGranularity(),
                  query.getDimensions(),
                  query.getAggregatorSpecs(),
                  // Don't do post aggs until the end of this method.
                  ImmutableList.<PostAggregator>of(),
                  // Don't do "having" clause until the end of this method.
                  null,
                  null,
                  query.getContext()
              ).withOverriddenContext(
                  ImmutableMap.<String, Object>of(
                      "finalize", false
                  )
              )
              , context
          )
      );
      return new ResourceClosingSequence<>(query.applyLimit(postAggregate(query, index)), index);
    }
  }

  private Sequence<Row> postAggregate(final GroupByQuery query, IncrementalIndex index)
  {
    return Sequences.map(
        Sequences.simple(index.iterableWithPostAggregations(query.getPostAggregatorSpecs(), query.isDescending())),
        new Function<Row, Row>()
        {
          @Override
          public Row apply(Row input)
          {
            final MapBasedRow row = (MapBasedRow) input;
            return new MapBasedRow(
                query.getGranularity()
                     .toDateTime(row.getTimestampFromEpoch()),
                row.getEvent()
            );
          }
        }
    );
  }

  private IncrementalIndex makeIncrementalIndex(GroupByQuery query, Sequence<Row> rows)
  {
    final GroupByQueryConfig config = configSupplier.get();
    Pair<IncrementalIndex, Accumulator<IncrementalIndex, Row>> indexAccumulatorPair = GroupByQueryHelper.createIndexAccumulatorPair(
        query,
        config,
        bufferPool
    );

    return rows.accumulate(indexAccumulatorPair.lhs, indexAccumulatorPair.rhs);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(GroupByQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query)
                       .setDimension("numDimensions", String.valueOf(query.getDimensions().size()))
                       .setDimension("numMetrics", String.valueOf(query.getAggregatorSpecs().size()))
                       .setDimension(
                           "numComplexMetrics",
                           String.valueOf(DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs()))
                       );
  }

  @Override
  public Function<Row, Row> makePreComputeManipulatorFn(
      final GroupByQuery query,
      final MetricManipulationFn fn
  )
  {
    return new Function<Row, Row>()
    {
      @Override
      public Row apply(Row input)
      {
        if (input instanceof MapBasedRow) {
          final MapBasedRow inputRow = (MapBasedRow) input;
          final Map<String, Object> values = Maps.newHashMap(inputRow.getEvent());
          for (AggregatorFactory agg : query.getAggregatorSpecs()) {
            values.put(agg.getName(), fn.manipulate(agg, inputRow.getEvent().get(agg.getName())));
          }
          return new MapBasedRow(inputRow.getTimestamp(), values);
        }
        return input;
      }
    };
  }

  @Override
  public Function<Row, Row> makePostComputeManipulatorFn(
      final GroupByQuery query,
      final MetricManipulationFn fn
  )
  {
    final Set<String> optimizedDims = ImmutableSet.copyOf(
        Iterables.transform(
            extractionsToRewrite(query),
            new Function<DimensionSpec, String>()
            {
              @Override
              public String apply(DimensionSpec input)
              {
                return input.getOutputName();
              }
            }
        )
    );
    final Function<Row, Row> preCompute = makePreComputeManipulatorFn(query, fn);
    if (optimizedDims.isEmpty()) {
      return preCompute;
    }

    // If we have optimizations that can be done at this level, we apply them here

    final Map<String, ExtractionFn> extractionFnMap = new HashMap<>();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      final String dimension = dimensionSpec.getOutputName();
      if (optimizedDims.contains(dimension)) {
        extractionFnMap.put(dimension, dimensionSpec.getExtractionFn());
      }
    }

    return new Function<Row, Row>()
    {
      @Nullable
      @Override
      public Row apply(Row input)
      {
        Row preRow = preCompute.apply(input);
        if (preRow instanceof MapBasedRow) {
          MapBasedRow preMapRow = (MapBasedRow) preRow;
          Map<String, Object> event = Maps.newHashMap(preMapRow.getEvent());
          for (String dim : optimizedDims) {
            final Object eventVal = event.get(dim);
            event.put(dim, extractionFnMap.get(dim).apply(eventVal));
          }
          return new MapBasedRow(preMapRow.getTimestamp(), event);
        } else {
          return preRow;
        }
      }
    };
  }

  @Override
  public TypeReference<Row> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public QueryRunner<Row> preMergeQueryDecoration(final QueryRunner<Row> runner)
  {
    return new SubqueryQueryRunner<>(
        intervalChunkingQueryRunnerDecorator.decorate(
            new QueryRunner<Row>()
            {
              @Override
              public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
              {
                if (!(query instanceof GroupByQuery)) {
                  return runner.run(query, responseContext);
                }
                GroupByQuery groupByQuery = (GroupByQuery) query;
                if (groupByQuery.getDimFilter() != null){
                  groupByQuery = groupByQuery.withDimFilter(groupByQuery.getDimFilter().optimize());
                }
                final GroupByQuery delegateGroupByQuery = groupByQuery;
                ArrayList<DimensionSpec> dimensionSpecs = new ArrayList<>();
                Set<String> optimizedDimensions = ImmutableSet.copyOf(
                    Iterables.transform(
                        extractionsToRewrite(delegateGroupByQuery),
                        new Function<DimensionSpec, String>()
                        {
                          @Override
                          public String apply(DimensionSpec input)
                          {
                            return input.getDimension();
                          }
                        }
                    )
                );
                for (DimensionSpec dimensionSpec : delegateGroupByQuery.getDimensions()) {
                  if (optimizedDimensions.contains(dimensionSpec.getDimension())) {
                    dimensionSpecs.add(
                        new DefaultDimensionSpec(dimensionSpec.getDimension(), dimensionSpec.getOutputName())
                    );
                  } else {
                    dimensionSpecs.add(dimensionSpec);
                  }
                }
                return runner.run(
                    delegateGroupByQuery.withDimensionSpecs(dimensionSpecs),
                    responseContext
                );
              }
            }, this
        )
    );
  }

  @Override
  public CacheStrategy<Row, Object, GroupByQuery> getCacheStrategy(final GroupByQuery query)
  {
    return new CacheStrategy<Row, Object, GroupByQuery>()
    {
      private static final byte CACHE_STRATEGY_VERSION = 0x1;
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();
      private final List<DimensionSpec> dims = query.getDimensions();


      @Override
      public byte[] computeCacheKey(GroupByQuery query)
      {
        final DimFilter dimFilter = query.getDimFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(query.getAggregatorSpecs());
        final byte[] granularityBytes = query.getGranularity().cacheKey();
        final byte[][] dimensionsBytes = new byte[query.getDimensions().size()][];
        int dimensionsBytesSize = 0;
        int index = 0;
        for (DimensionSpec dimension : query.getDimensions()) {
          dimensionsBytes[index] = dimension.getCacheKey();
          dimensionsBytesSize += dimensionsBytes[index].length;
          ++index;
        }
        final byte[] havingBytes = query.getHavingSpec() == null ? new byte[]{} : query.getHavingSpec().getCacheKey();
        final byte[] limitBytes = query.getLimitSpec().getCacheKey();

        ByteBuffer buffer = ByteBuffer
            .allocate(
                2
                + granularityBytes.length
                + filterBytes.length
                + aggregatorBytes.length
                + dimensionsBytesSize
                + havingBytes.length
                + limitBytes.length
            )
            .put(GROUPBY_QUERY)
            .put(CACHE_STRATEGY_VERSION)
            .put(granularityBytes)
            .put(filterBytes)
            .put(aggregatorBytes);

        for (byte[] dimensionsByte : dimensionsBytes) {
          buffer.put(dimensionsByte);
        }

        return buffer
            .put(havingBytes)
            .put(limitBytes)
            .array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Row, Object> prepareForCache()
      {
        return new Function<Row, Object>()
        {
          @Override
          public Object apply(Row input)
          {
            if (input instanceof MapBasedRow) {
              final MapBasedRow row = (MapBasedRow) input;
              final List<Object> retVal = Lists.newArrayListWithCapacity(1 + dims.size() + aggs.size());
              retVal.add(row.getTimestamp().getMillis());
              Map<String, Object> event = row.getEvent();
              for (DimensionSpec dim : dims) {
                retVal.add(event.get(dim.getOutputName()));
              }
              for (AggregatorFactory agg : aggs) {
                retVal.add(event.get(agg.getName()));
              }
              return retVal;
            }

            throw new ISE("Don't know how to cache input rows of type[%s]", input.getClass());
          }
        };
      }

      @Override
      public Function<Object, Row> pullFromCache()
      {
        return new Function<Object, Row>()
        {
          private final QueryGranularity granularity = query.getGranularity();

          @Override
          public Row apply(Object input)
          {
            Iterator<Object> results = ((List<Object>) input).iterator();

            DateTime timestamp = granularity.toDateTime(((Number) results.next()).longValue());

            Map<String, Object> event = Maps.newLinkedHashMap();
            Iterator<DimensionSpec> dimsIter = dims.iterator();
            while (dimsIter.hasNext() && results.hasNext()) {
              final DimensionSpec factory = dimsIter.next();
              event.put(factory.getOutputName(), results.next());
            }

            Iterator<AggregatorFactory> aggsIter = aggs.iterator();
            while (aggsIter.hasNext() && results.hasNext()) {
              final AggregatorFactory factory = aggsIter.next();
              event.put(factory.getName(), factory.deserialize(results.next()));
            }

            if (dimsIter.hasNext() || aggsIter.hasNext() || results.hasNext()) {
              throw new ISE(
                  "Found left over objects while reading from cache!! dimsIter[%s] aggsIter[%s] results[%s]",
                  dimsIter.hasNext(),
                  aggsIter.hasNext(),
                  results.hasNext()
              );
            }

            return new MapBasedRow(
                timestamp,
                event
            );
          }
        };
      }
    };
  }


  /**
   * This function checks the query for dimensions which can be optimized by applying the dimension extraction
   * as the final step of the query instead of on every event.
   *
   * @param query The query to check for optimizations
   *
   * @return A collection of DimensionsSpec which can be extracted at the last second upon query completion.
   */
  public static Collection<DimensionSpec> extractionsToRewrite(GroupByQuery query)
  {
    return Collections2.filter(
        query.getDimensions(), new Predicate<DimensionSpec>()
        {
          @Override
          public boolean apply(DimensionSpec input)
          {
            return input.getExtractionFn() != null
                   && ExtractionFn.ExtractionType.ONE_TO_ONE.equals(
                input.getExtractionFn().getExtractionType()
            );
          }
        }
    );
  }
}

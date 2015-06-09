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
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.guice.annotations.Global;
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
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.DateTime;

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
      public Sequence<Row> run(Query<Row> input, Map<String, Object> responseContext)
      {
        if (input.getContextBySegment(false)) {
          return runner.run(input, responseContext);
        }

        if (Boolean.valueOf(input.getContextValue(GROUP_BY_MERGE_KEY, "true"))) {
          return mergeGroupByResults(
              (GroupByQuery) input,
              runner,
              responseContext
          );
        }
        return runner.run(input, responseContext);
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
      final List<AggregatorFactory> aggs = Lists.newArrayList();
      for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
        aggs.addAll(aggregatorFactory.getRequiredColumns());
      }

      // We need the inner incremental index to have all the columns required by the outer query
      final GroupByQuery innerQuery = new GroupByQuery.Builder(query)
          .setAggregatorSpecs(aggs)
          .setInterval(subquery.getIntervals())
          .setPostAggregatorSpecs(Lists.<PostAggregator>newArrayList())
          .build();

      final GroupByQuery outerQuery = new GroupByQuery.Builder(query)
          .setLimitSpec(query.getLimitSpec().merge(subquery.getLimitSpec()))
          .build();
      IncrementalIndex index = makeIncrementalIndex(innerQuery, subqueryResult);

      return new ResourceClosingSequence<>(
          outerQuery.applyLimit(
              engine.process(
                  outerQuery,
                  new IncrementalIndexStorageAdapter(
                      index
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
        Sequences.simple(index.iterableWithPostAggregations(query.getPostAggregatorSpecs())),
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
  public Sequence<Row> mergeSequences(Sequence<Sequence<Row>> seqOfSequences)
  {
    return new OrderedMergeSequence<>(getOrdering(), seqOfSequences);
  }

  @Override
  public Sequence<Row> mergeSequencesUnordered(Sequence<Sequence<Row>> seqOfSequences)
  {
    return new MergeSequence<>(getOrdering(), seqOfSequences);
  }

  private Ordering<Row> getOrdering()
  {
    return Ordering.<Row>natural().nullsFirst();
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
                ArrayList<DimensionSpec> dimensionSpecs = new ArrayList<>();
                Set<String> optimizedDimensions = ImmutableSet.copyOf(
                    Iterables.transform(
                        extractionsToRewrite(groupByQuery),
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
                for (DimensionSpec dimensionSpec : groupByQuery.getDimensions()) {
                  if (optimizedDimensions.contains(dimensionSpec.getDimension())) {
                    dimensionSpecs.add(
                        new DefaultDimensionSpec(dimensionSpec.getDimension(), dimensionSpec.getOutputName())
                    );
                  } else {
                    dimensionSpecs.add(dimensionSpec);
                  }
                }
                return runner.run(
                    groupByQuery.withDimensionSpecs(dimensionSpecs),
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
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();

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
                1
                + granularityBytes.length
                + filterBytes.length
                + aggregatorBytes.length
                + dimensionsBytesSize
                + havingBytes.length
                + limitBytes.length
            )
            .put(GROUPBY_QUERY)
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
              final List<Object> retVal = Lists.newArrayListWithCapacity(2);
              retVal.add(row.getTimestamp().getMillis());
              retVal.add(row.getEvent());

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

            Iterator<AggregatorFactory> aggsIter = aggs.iterator();

            Map<String, Object> event = jsonMapper.convertValue(
                results.next(),
                new TypeReference<Map<String, Object>>()
                {
                }
            );

            while (aggsIter.hasNext()) {
              final AggregatorFactory factory = aggsIter.next();
              Object agg = event.get(factory.getName());
              if (agg != null) {
                event.put(factory.getName(), factory.deserialize(agg));
              }
            }

            return new MapBasedRow(
                timestamp,
                event
            );
          }
        };
      }

      @Override
      public Sequence<Row> mergeSequences(Sequence<Sequence<Row>> seqOfSequences)
      {
        return new MergeSequence<>(getOrdering(), seqOfSequences);
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

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
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
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
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
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
  public static final String GROUP_BY_MERGE_KEY = "groupByMerge";

  private final Supplier<GroupByQueryConfig> configSupplier;
  private final GroupByStrategySelector strategySelector;
  private final StupidPool<ByteBuffer> bufferPool;
  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public GroupByQueryQueryToolChest(
      Supplier<GroupByQueryConfig> configSupplier,
      GroupByStrategySelector strategySelector,
      @Global StupidPool<ByteBuffer> bufferPool,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.configSupplier = configSupplier;
    this.strategySelector = strategySelector;
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

        if (query.getContextBoolean(GROUP_BY_MERGE_KEY, true)) {
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
      final GroupByQuery subquery;
      try {
        // Inject outer query context keys into subquery if they don't already exist in the subquery context.
        // Unlike withOverriddenContext's normal behavior, we want keys present in the subquery to win.
        final Map<String, Object> subqueryContext = Maps.newTreeMap();
        if (query.getContext() != null) {
          for (Map.Entry<String, Object> entry : query.getContext().entrySet()) {
            if (entry.getValue() != null) {
              subqueryContext.put(entry.getKey(), entry.getValue());
            }
          }
        }
        if (((QueryDataSource) dataSource).getQuery().getContext() != null) {
          subqueryContext.putAll(((QueryDataSource) dataSource).getQuery().getContext());
        }
        subquery = (GroupByQuery) ((QueryDataSource) dataSource).getQuery().withOverriddenContext(subqueryContext);
      }
      catch (ClassCastException e) {
        throw new UnsupportedOperationException("Subqueries must be of type 'group by'");
      }

      final Sequence<Row> subqueryResult = mergeGroupByResults(
          subquery.withOverriddenContext(
              ImmutableMap.<String, Object>of(
                  //setting sort to false avoids unnecessary sorting while merging results. we only need to sort
                  //in the end when returning results to user.
                  GroupByQueryHelper.CTX_KEY_SORT_RESULTS,
                  false
              )
          ),
          runner,
          context
      );
      return strategySelector.strategize(query).processSubqueryResult(subquery, query, subqueryResult);
    } else {
      return strategySelector.strategize(query).mergeResults(runner, query, context);
    }
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
    if (MetricManipulatorFns.identity().equals(fn)) {
      return Functions.identity();
    }

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
                GroupByQuery groupByQuery = (GroupByQuery) query;
                if (groupByQuery.getDimFilter() != null) {
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

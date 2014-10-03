/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.groupby;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.CacheStrategy;
import io.druid.query.DataSource;
import io.druid.query.IntervalChunkingQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryMetricUtil;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.SubqueryQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
  private static final Map<String, Object> NO_MERGE_CONTEXT = ImmutableMap.<String, Object>of(
      GROUP_BY_MERGE_KEY,
      "false"
  );

  private final Supplier<GroupByQueryConfig> configSupplier;
  private final ObjectMapper jsonMapper;
  private GroupByQueryEngine engine; // For running the outer query around a subquery

  @Inject
  public GroupByQueryQueryToolChest(
      Supplier<GroupByQueryConfig> configSupplier,
      ObjectMapper jsonMapper,
      GroupByQueryEngine engine
  )
  {
    this.configSupplier = configSupplier;
    this.jsonMapper = jsonMapper;
    this.engine = engine;
  }

  @Override
  public QueryRunner<Row> mergeResults(final QueryRunner<Row> runner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> input)
      {
        if (input.getContextBySegment(false)) {
          return runner.run(input);
        }

        if (Boolean.valueOf(input.getContextValue(GROUP_BY_MERGE_KEY, "true"))) {
          return mergeGroupByResults(((GroupByQuery) input).withOverriddenContext(NO_MERGE_CONTEXT), runner);
        }
        return runner.run(input);
      }
    };
  }

  private Sequence<Row> mergeGroupByResults(final GroupByQuery query, QueryRunner<Row> runner)
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
      final Sequence<Row> subqueryResult = mergeGroupByResults(subquery, runner);
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

      final IncrementalIndexStorageAdapter adapter = new IncrementalIndexStorageAdapter(
          makeIncrementalIndex(innerQuery, subqueryResult)
      );
      return outerQuery.applyLimit(engine.process(outerQuery, adapter));
    } else {
      return query.applyLimit(postAggregate(query, makeIncrementalIndex(query, runner.run(query))));
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
        config
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
    return QueryMetricUtil.makeQueryTimeMetric(query)
                          .setUser3(String.format("%,d dims", query.getDimensions().size()))
                          .setUser7(String.format("%,d aggs", query.getAggregatorSpecs().size()));
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
  public TypeReference<Row> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public QueryRunner<Row> preMergeQueryDecoration(QueryRunner<Row> runner)
  {
    return new SubqueryQueryRunner<>(
        new IntervalChunkingQueryRunner<>(runner, configSupplier.get().getChunkPeriod())
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
}

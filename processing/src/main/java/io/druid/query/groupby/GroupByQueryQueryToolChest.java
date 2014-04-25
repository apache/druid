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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.ConcatSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.DataSource;
import io.druid.query.IntervalChunkingQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.SubqueryQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.Interval;
import org.joda.time.Minutes;

import java.util.Map;

/**
 */
public class GroupByQueryQueryToolChest extends QueryToolChest<Row, GroupByQuery>
{
  private static final TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>()
  {
  };
  private static final String GROUP_BY_MERGE_KEY = "groupByMerge";
  private static final Map<String, Object> NO_MERGE_CONTEXT = ImmutableMap.<String, Object>of(GROUP_BY_MERGE_KEY, "false");
  private final Supplier<GroupByQueryConfig> configSupplier;
  private GroupByQueryEngine engine; // For running the outer query around a subquery

  @Inject
  public GroupByQueryQueryToolChest(
      Supplier<GroupByQueryConfig> configSupplier,
      GroupByQueryEngine engine
  )
  {
    this.configSupplier = configSupplier;
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
        if (Boolean.valueOf((String) input.getContextValue(GROUP_BY_MERGE_KEY, "true"))) {
          return mergeGroupByResults(((GroupByQuery) input).withOverriddenContext(NO_MERGE_CONTEXT), runner);
        } else {
          return runner.run(input);
        }
      }
    };
  }

  private Sequence<Row> mergeGroupByResults(final GroupByQuery query, QueryRunner<Row> runner)
  {

    Sequence<Row> result;

    // If there's a subquery, merge subquery results and then apply the aggregator
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof QueryDataSource) {
      GroupByQuery subquery;
      try {
        subquery = (GroupByQuery) ((QueryDataSource) dataSource).getQuery();
      } catch (ClassCastException e) {
        throw new UnsupportedOperationException("Subqueries must be of type 'group by'");
      }
      Sequence<Row> subqueryResult = mergeGroupByResults(subquery, runner);
      IncrementalIndexStorageAdapter adapter
          = new IncrementalIndexStorageAdapter(makeIncrementalIndex(subquery, subqueryResult));
      result = engine.process(query, adapter);
    } else {
      result = runner.run(query);
    }

    return postAggregate(query, makeIncrementalIndex(query, result));
  }


  private Sequence<Row> postAggregate(final GroupByQuery query, IncrementalIndex index)
  {
    Sequence<Row> sequence = Sequences.map(
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
    return query.applyLimit(sequence);
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
    return new ConcatSequence<Row>(seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(GroupByQuery query)
  {
    int numMinutes = 0;
    for (Interval interval : query.getIntervals()) {
      numMinutes += Minutes.minutesIn(interval).getMinutes();
    }

    return new ServiceMetricEvent.Builder()
        .setUser2(query.getDataSource().toString())
        .setUser3(String.format("%,d dims", query.getDimensions().size()))
        .setUser4("groupBy")
        .setUser5(Joiner.on(",").join(query.getIntervals()))
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser7(String.format("%,d aggs", query.getAggregatorSpecs().size()))
        .setUser9(Minutes.minutes(numMinutes).toString());
  }

  @Override
  public Function<Row, Row> makePreComputeManipulatorFn(final GroupByQuery query, final MetricManipulationFn fn)
  {
    return new Function<Row, Row>()
    {
      @Override
      public Row apply(Row input)
      {
        if (input instanceof MapBasedRow) {
          final MapBasedRow inputRow = (MapBasedRow) input;
          final Map<String, Object> values = Maps.newHashMap(((MapBasedRow) input).getEvent());
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
    return new SubqueryQueryRunner<Row>(
        new IntervalChunkingQueryRunner<Row>(runner, configSupplier.get().getChunkPeriod()));
  }
}

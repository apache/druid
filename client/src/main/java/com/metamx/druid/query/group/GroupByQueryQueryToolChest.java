/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.group;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.ConcatSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Query;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.index.v1.IncrementalIndex;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.input.MapBasedRow;
import com.metamx.druid.input.Row;
import com.metamx.druid.input.Rows;
import com.metamx.druid.query.CacheStrategy;
import com.metamx.druid.query.MetricManipulationFn;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.dimension.DimensionSpec;
import com.metamx.druid.query.having.HavingSpec;
import com.metamx.druid.query.order.TopNSorter;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.Interval;
import org.joda.time.Minutes;

import javax.annotation.Nullable;
import java.util.*;

/**
 */
public class GroupByQueryQueryToolChest implements QueryToolChest<Row, GroupByQuery>
{

  private static final TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>(){};

  private static final int maxRows;

  static {
    // I dislike this static loading of properies, but it's the only mechanism available right now.
    Properties props = Initialization.loadProperties();

    maxRows = PropUtils.getPropertyAsInt(props, "com.metamx.query.groupBy.maxResults", 500000);
  }

  @Override
  public QueryRunner<Row> mergeResults(final QueryRunner<Row> runner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> input)
      {
        final GroupByQuery query = (GroupByQuery) input;

        List<Interval> condensed = query.getIntervals();
        final List<AggregatorFactory> aggs = Lists.transform(
            query.getAggregatorSpecs(),
            new Function<AggregatorFactory, AggregatorFactory>()
            {
              @Override
              public AggregatorFactory apply(@Nullable AggregatorFactory input)
              {
                return input.getCombiningFactory();
              }
            }
        );
        final List<String> dimensions = Lists.transform(
            query.getDimensions(),
            new Function<DimensionSpec, String>()
            {
              @Override
              public String apply(@Nullable DimensionSpec input)
              {
                return input.getOutputName();
              }
            }
        );

        final QueryGranularity gran = query.getGranularity();
        final IncrementalIndex index = runner.run(query).accumulate(
            new IncrementalIndex(
                gran.truncate(condensed.get(0).getStartMillis()),
                gran,
                aggs.toArray(new AggregatorFactory[aggs.size()])
            ),
            new Accumulator<IncrementalIndex, Row>()
            {
              @Override
              public IncrementalIndex accumulate(IncrementalIndex accumulated, Row in)
              {
                if (accumulated.add(Rows.toInputRow(in, dimensions)) > maxRows) {
                  throw new ISE("Computation exceeds maxRows limit[%s]", maxRows);
                }

                return accumulated;
              }
            }
        );

        final Iterable<Row> filtered = filterByHavingSpec(
          query.getHavingSpec(),
          index.iterableWithPostAggregations(query.getPostAggregatorSpecs()));

        final Iterable<Row> ordered = applyOrderByAndLimit(
          query,
          filtered);

        // convert millis back to timestamp according to granularity to preserve time zone information
        return Sequences.map(
            Sequences.simple(ordered),
            new Function<Row, Row>()
            {
              private final QueryGranularity granularity = query.getGranularity();

              @Override
              public Row apply(Row input)
              {
                final MapBasedRow row = (MapBasedRow) input;
                return new MapBasedRow(granularity.toDateTime(row.getTimestampFromEpoch()), row.getEvent());
              }
            }
        );
      }
    };
  }

  private Iterable<Row> filterByHavingSpec(final HavingSpec havingSpec, final Iterable<Row> rows)
  {
    // If there's no having clause, we don't need to iterate over the whole iterable. Just a
    // simple optimization.
    if(havingSpec == null) {
      return rows;
    }

    return Iterables.filter(
      rows,
      new Predicate<Row>()
      {
        @Override
        public boolean apply(@Nullable final Row row)
        {
          return havingSpec.eval(row);
        }
      }
    );
  }


  private Iterable<Row> applyOrderByAndLimit(GroupByQuery query, Iterable<Row> rows)
  {
    if(query.getOrderBy() == null && query.getLimit() == null) {
      return rows;
    }

    if(query.getOrderBy() == null) {
      return Iterables.limit(rows, query.getLimit().intValue());
    }

    TopNSorter sorter = new TopNSorter(query.getOrderBy().getRowOrdering());

    if(query.getLimit() == null) {
      return sorter.sortedCopy(rows);
    }

    return sorter.toTopN(rows, query.getLimit().intValue());
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
        .setUser2(query.getDataSource())
        .setUser3(String.format("%,d dims", query.getDimensions().size()))
        .setUser4("groupBy")
        .setUser5(Joiner.on(",").join(query.getIntervals()))
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser7(String.format("%,d aggs", query.getAggregatorSpecs().size()))
        .setUser9(Minutes.minutes(numMinutes).toString());
  }

  @Override
  public Function<Row, Row> makeMetricManipulatorFn(GroupByQuery query, MetricManipulationFn fn)
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Row> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Row, Object, GroupByQuery> getCacheStrategy(GroupByQuery query)
  {
    return null;
  }

  @Override
  public QueryRunner<Row> preMergeQueryDecoration(QueryRunner<Row> runner)
  {
    return runner;
  }

  @Override
  public QueryRunner<Row> postMergeQueryDecoration(QueryRunner<Row> runner)
  {
    return runner;
  }
}

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

package io.druid.query.topn;

import com.metamx.common.Pair;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 */
public abstract class BaseTopNAlgorithm<DimValSelector, DimValAggregateStore, Parameters extends TopNParams>
    implements TopNAlgorithm<DimValSelector, Parameters>
{
  protected static Aggregator[] makeAggregators(Cursor cursor, List<AggregatorFactory> aggregatorSpecs)
  {
    Aggregator[] aggregators = new Aggregator[aggregatorSpecs.size()];
    int aggregatorIndex = 0;
    for (AggregatorFactory spec : aggregatorSpecs) {
      aggregators[aggregatorIndex] = spec.factorize(cursor);
      ++aggregatorIndex;
    }
    return aggregators;
  }

  protected static BufferAggregator[] makeBufferAggregators(Cursor cursor, List<AggregatorFactory> aggregatorSpecs)
  {
    BufferAggregator[] aggregators = new BufferAggregator[aggregatorSpecs.size()];
    int aggregatorIndex = 0;
    for (AggregatorFactory spec : aggregatorSpecs) {
      aggregators[aggregatorIndex] = spec.factorizeBuffered(cursor);
      ++aggregatorIndex;
    }
    return aggregators;
  }

  private final Capabilities capabilities;

  protected BaseTopNAlgorithm(Capabilities capabilities)
  {
    this.capabilities = capabilities;
  }

  @Override
  public void run(
      Parameters params,
      TopNResultBuilder resultBuilder,
      DimValSelector dimValSelector
  )
  {
    boolean hasDimValSelector = (dimValSelector != null);

    final int cardinality = params.getCardinality();
    int numProcessed = 0;
    while (numProcessed < cardinality) {
      final int numToProcess = Math.min(params.getNumValuesPerPass(), cardinality - numProcessed);

      DimValSelector theDimValSelector;
      if (!hasDimValSelector) {
        theDimValSelector = makeDimValSelector(params, numProcessed, numToProcess);
      } else {
        theDimValSelector = updateDimValSelector(dimValSelector, numProcessed, numToProcess);
      }

      DimValAggregateStore aggregatesStore = makeDimValAggregateStore(params);

      scanAndAggregate(params, theDimValSelector, aggregatesStore, numProcessed);

      updateResults(params, theDimValSelector, aggregatesStore, resultBuilder);

      closeAggregators(aggregatesStore);

      numProcessed += numToProcess;
      params.getCursor().reset();
    }
  }

  protected abstract DimValSelector makeDimValSelector(Parameters params, int numProcessed, int numToProcess);

  protected abstract DimValSelector updateDimValSelector(
      DimValSelector dimValSelector,
      int numProcessed,
      int numToProcess
  );

  protected abstract DimValAggregateStore makeDimValAggregateStore(Parameters params);

  protected abstract void scanAndAggregate(
      Parameters params,
      DimValSelector dimValSelector,
      DimValAggregateStore dimValAggregateStore,
      int numProcessed
  );

  protected abstract void updateResults(
      Parameters params,
      DimValSelector dimValSelector,
      DimValAggregateStore dimValAggregateStore,
      TopNResultBuilder resultBuilder
  );

  protected abstract void closeAggregators(
      DimValAggregateStore dimValAggregateStore
  );

  protected class AggregatorArrayProvider extends BaseArrayProvider<Aggregator[][]>
  {
    Aggregator[][] expansionAggs;
    int cardinality;

    public AggregatorArrayProvider(DimensionSelector dimSelector, TopNQuery query, int cardinality)
    {
      super(dimSelector, query, capabilities);

      this.expansionAggs = new Aggregator[cardinality][];
      this.cardinality = cardinality;
    }

    @Override
    public Aggregator[][] build()
    {
      Pair<Integer, Integer> startEnd = computeStartEnd(cardinality);

      Arrays.fill(expansionAggs, 0, startEnd.lhs, EMPTY_ARRAY);
      Arrays.fill(expansionAggs, startEnd.lhs, startEnd.rhs, null);
      Arrays.fill(expansionAggs, startEnd.rhs, expansionAggs.length, EMPTY_ARRAY);

      return expansionAggs;
    }
  }

  protected static abstract class BaseArrayProvider<T> implements TopNMetricSpecBuilder<T>
  {
    private volatile String previousStop;
    private volatile boolean ignoreAfterThreshold;
    private volatile int ignoreFirstN;
    private volatile int keepOnlyN;

    private final DimensionSelector dimSelector;
    private final TopNQuery query;
    private final Capabilities capabilities;

    public BaseArrayProvider(
        DimensionSelector dimSelector,
        TopNQuery query,
        Capabilities capabilities
    )
    {
      this.dimSelector = dimSelector;
      this.query = query;
      this.capabilities = capabilities;

      previousStop = null;
      ignoreAfterThreshold = false;
      ignoreFirstN = 0;
      keepOnlyN = dimSelector.getValueCardinality();
    }

    @Override
    public void skipTo(String previousStop)
    {
      if (capabilities.dimensionValuesSorted()) {
        this.previousStop = previousStop;
      }
    }

    @Override
    public void ignoreAfterThreshold()
    {
      ignoreAfterThreshold = true;
    }

    @Override
    public void ignoreFirstN(int n)
    {
      ignoreFirstN = n;
    }

    @Override
    public void keepOnlyN(int n)
    {
      keepOnlyN = n;
    }

    protected Pair<Integer, Integer> computeStartEnd(int cardinality)
    {
      int startIndex = ignoreFirstN;

      if (previousStop != null) {
        int lookupId = dimSelector.lookupId(previousStop) + 1;
        if (lookupId < 0) {
          lookupId *= -1;
        }
        if (lookupId > ignoreFirstN + keepOnlyN) {
          startIndex = ignoreFirstN + keepOnlyN;
        } else {
          startIndex = Math.max(lookupId, startIndex);
        }
      }

      int endIndex = Math.min(ignoreFirstN + keepOnlyN, cardinality);

      if (ignoreAfterThreshold && query.getDimensionsFilter() == null) {
        endIndex = Math.min(endIndex, startIndex + query.getThreshold());
      }

      return Pair.of(startIndex, endIndex);
    }
  }

  public static TopNResultBuilder makeResultBuilder(TopNParams params, TopNQuery query)
  {
    Comparator comparator = query.getTopNMetricSpec()
                                 .getComparator(query.getAggregatorSpecs(), query.getPostAggregatorSpecs());
    return query.getTopNMetricSpec().getResultBuilder(
        params.getCursor().getTime(),
        query.getDimensionSpec(),
        query.getThreshold(),
        comparator,
        query.getAggregatorSpecs(),
        query.getPostAggregatorSpecs()
    );
  }
}

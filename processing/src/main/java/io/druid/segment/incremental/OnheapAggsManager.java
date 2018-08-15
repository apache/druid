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

package io.druid.segment.incremental;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.druid.segment.column.ColumnCapabilitiesImpl;

public class OnheapAggsManager extends AggsManager<Aggregator>
{
  private static final Logger log = new Logger(OnheapAggsManager.class);

  private volatile Map<String, ColumnSelectorFactory> selectors;
  private final ConcurrentHashMap<Integer, Aggregator[]> aggregators;

  /* basic constractor */
  OnheapAggsManager(
          final IncrementalIndexSchema incrementalIndexSchema,
          final boolean deserializeComplexMetrics,
          final boolean reportParseExceptions,
          final boolean concurrentEventAdd,
          Supplier<InputRow> rowSupplier,
          Map<String, ColumnCapabilitiesImpl> columnCapabilities,
          IncrementalIndex incrementalIndex
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions,
            concurrentEventAdd, rowSupplier, columnCapabilities, incrementalIndex);
    this.aggregators = new ConcurrentHashMap<>();
  }

  public void closeAggregators()
  {
    Closer closer = Closer.create();
    for (Aggregator[] aggs : aggregators.values()) {
      for (Aggregator agg : aggs) {
        closer.register(agg);
      }
    }

    try {
      closer.close();
    }
    catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  public void clearAggregators()
  {
    aggregators.clear();
  }

  public void clearSelectors()
  {
    if (selectors != null) {
      selectors.clear();
    }
  }

  public Aggregator[] concurrentGet(int offset)
  {
    // All get operations should be fine
    return aggregators.get(offset);
  }

  public void concurrentSet(int offset, Aggregator[] value)
  {
    aggregators.put(offset, value);
  }

  public void concurrentRemove(int offset)
  {
    aggregators.remove(offset);
  }

  public Aggregator[] concurrentPutIfAbsent(int offset, Aggregator[] value)
  {
    return aggregators.putIfAbsent(offset, value);
  }

  @Override
  public Aggregator[] initAggs(
          AggregatorFactory[] metrics,
          Supplier<InputRow> rowSupplier,
          boolean deserializeComplexMetrics,
          boolean concurrentEventAdd
  )
  {
    selectors = Maps.newHashMap();
    for (AggregatorFactory agg : metrics) {
      selectors.put(
              agg.getName(),
              new OnheapIncrementalIndex.ObjectCachingColumnSelectorFactory(
                      makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics),
                      concurrentEventAdd
              )
      );
    }

    return new Aggregator[metrics.length];
  }

  public void factorizeAggs(
          AggregatorFactory[] metrics,
          Aggregator[] aggs,
          ThreadLocal<InputRow> rowContainer,
          InputRow row
  )
  {
    rowContainer.set(row);
    for (int i = 0; i < metrics.length; i++) {
      final AggregatorFactory agg = metrics[i];
      aggs[i] = agg.factorize(selectors.get(agg.getName()));
    }
    rowContainer.set(null);
  }

  public List<String> doAggregate(
          AggregatorFactory[] metrics,
          Aggregator[] aggs,
          ThreadLocal<InputRow> rowContainer,
          InputRow row
  )
  {
    List<String> parseExceptionMessages = new ArrayList<>();
    rowContainer.set(row);

    for (int i = 0; i < aggs.length; i++) {
      final Aggregator agg = aggs[i];
      synchronized (agg) {
        try {
          agg.aggregate();
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          log.debug(e, "Encountered parse error, skipping aggregator[%s].", metrics[i].getName());
          parseExceptionMessages.add(e.getMessage());
        }
      }
    }

    rowContainer.set(null);
    return parseExceptionMessages;
  }

}

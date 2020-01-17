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

package org.apache.druid.query.topn;

import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.topn.types.TopNColumnAggregatesProcessor;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;

/**
 * Heap based topn algorithm that handles aggregates on dimension extractions and numeric typed dimension columns.
 *
 * This has to be its own strategy because the pooled topn algorithm assumes each index is unique, and cannot handle
 * multiple index numerals referencing the same dimension value.
 */
public class HeapBasedTopNAlgorithm
    extends BaseTopNAlgorithm<Aggregator[][], TopNColumnAggregatesProcessor, TopNParams>
{
  private final TopNQuery query;

  public HeapBasedTopNAlgorithm(
      StorageAdapter storageAdapter,
      TopNQuery query
  )
  {
    super(storageAdapter);

    this.query = query;
  }

  @Override
  public TopNParams makeInitParams(
      final ColumnSelectorPlus<TopNColumnAggregatesProcessor> selectorPlus,
      final Cursor cursor
  )
  {
    return new TopNParams(
        selectorPlus,
        cursor,
        Integer.MAX_VALUE
    );
  }

  @Override
  protected Aggregator[][] makeDimValSelector(TopNParams params, int numProcessed, int numToProcess)
  {
    if (params.getCardinality() < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with unknown cardinality");
    }
    ColumnSelectorPlus<TopNColumnAggregatesProcessor> selectorPlus = params.getSelectorPlus();
    return selectorPlus.getColumnSelectorStrategy().getRowSelector(query, params, storageAdapter);
  }

  @Override
  protected Aggregator[][] updateDimValSelector(Aggregator[][] aggregators, int numProcessed, int numToProcess)
  {
    return aggregators;
  }

  @Override
  protected TopNColumnAggregatesProcessor makeDimValAggregateStore(TopNParams params)
  {
    final ColumnSelectorPlus<TopNColumnAggregatesProcessor> selectorPlus = params.getSelectorPlus();
    return selectorPlus.getColumnSelectorStrategy();
  }

  @Override
  protected long scanAndAggregate(
      TopNParams params,
      Aggregator[][] rowSelector,
      TopNColumnAggregatesProcessor processor
  )
  {
    final Cursor cursor = params.getCursor();
    final ColumnSelectorPlus<TopNColumnAggregatesProcessor> selectorPlus = params.getSelectorPlus();

    processor.initAggregateStore();
    return processor.scanAndAggregate(
        query,
        selectorPlus.getSelector(),
        cursor,
        rowSelector
    );
  }

  @Override
  protected void updateResults(
      TopNParams params,
      Aggregator[][] aggregators,
      TopNColumnAggregatesProcessor processor,
      TopNResultBuilder resultBuilder
  )
  {
    processor.updateResults(resultBuilder);
  }

  @Override
  protected void closeAggregators(TopNColumnAggregatesProcessor processor)
  {
    processor.closeAggregators();
  }

  @Override
  public void cleanup(TopNParams params)
  {
  }
}

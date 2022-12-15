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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.rowsandcols.OnHeapCumulativeAggregatable;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultOnHeapAggregatable implements OnHeapAggregatable, OnHeapCumulativeAggregatable
{
  private final RowsAndColumns rac;

  public DefaultOnHeapAggregatable(
      RowsAndColumns rac
  )
  {
    this.rac = rac;
  }

  @Override
  public ArrayList<Object> aggregateAll(
      List<AggregatorFactory> aggFactories
  )
  {
    Aggregator[] aggs = new Aggregator[aggFactories.size()];

    AtomicInteger currRow = new AtomicInteger(0);
    int index = 0;
    for (AggregatorFactory aggFactory : aggFactories) {
      aggs[index++] = aggFactory.factorize(new DefaultColumnSelectorFactoryMaker.ColumnAccessorBasedColumnSelectorFactory(
          currRow,
          rac
      ));
    }

    int numRows = rac.numRows();
    int rowId = currRow.get();
    while (rowId < numRows) {
      for (Aggregator agg : aggs) {
        agg.aggregate();
      }
      rowId = currRow.incrementAndGet();
    }

    ArrayList<Object> retVal = new ArrayList<>(aggs.length);
    for (Aggregator agg : aggs) {
      retVal.add(agg.get());
      agg.close();
    }
    return retVal;
  }

  @Override
  public ArrayList<Object[]> aggregateCumulative(List<AggregatorFactory> aggFactories)
  {
    Aggregator[] aggs = new Aggregator[aggFactories.size()];
    ArrayList<Object[]> retVal = new ArrayList<>(aggFactories.size());

    int numRows = rac.numRows();
    AtomicInteger currRow = new AtomicInteger(0);
    int index = 0;
    for (AggregatorFactory aggFactory : aggFactories) {
      aggs[index++] = aggFactory.factorize(new DefaultColumnSelectorFactoryMaker.ColumnAccessorBasedColumnSelectorFactory(
          currRow,
          rac
      ));
      retVal.add(new Object[numRows]);
    }

    int rowId = currRow.get();
    while (rowId < numRows) {
      for (int i = 0; i < aggs.length; ++i) {
        aggs[i].aggregate();
        retVal.get(i)[rowId] = aggs[i].get();
      }
      rowId = currRow.incrementAndGet();
    }

    for (Aggregator agg : aggs) {
      agg.close();
    }

    return retVal;
  }

}

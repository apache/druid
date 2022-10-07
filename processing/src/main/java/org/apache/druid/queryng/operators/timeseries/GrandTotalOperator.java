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

package org.apache.druid.queryng.operators.timeseries;

import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operator.IterableOperator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.ResultIterator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implement a time-series grand total: emit all input rows followed
 * by a final row with totals for aggregations.
 *
 * @see {@link org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest#mergeResults
 * TimeseriesQueryQueryToolChest.mergeResults}
 */
public class GrandTotalOperator implements IterableOperator<Result<TimeseriesResultValue>>
{
  private final FragmentContext context;
  private final Operator<Result<TimeseriesResultValue>> input;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final Object[] grandTotals;
  private ResultIterator<Result<TimeseriesResultValue>> inputIter;
  private int rowCount;

  public GrandTotalOperator(
      FragmentContext fragmentContext,
      Operator<Result<TimeseriesResultValue>> op,
      List<AggregatorFactory> aggregatorSpecs
  )
  {
    this.context = fragmentContext;
    this.input = op;
    this.aggregatorSpecs = aggregatorSpecs;
    this.grandTotals = new Object[aggregatorSpecs.size()];
    context.register(this);
    context.registerChild(this, input);
  }

  @Override
  public ResultIterator<Result<TimeseriesResultValue>> open()
  {
    inputIter = input.open();
    return this;
  }

  @Override
  public Result<TimeseriesResultValue> next() throws ResultIterator.EofException
  {
    if (inputIter == null) {
      // All rows delivered.
      throw Operators.eof();
    }

    try {
      // Accumulate grand totals while iterating the input.
      Result<TimeseriesResultValue> resultValue = inputIter.next();
      rowCount++;
      accumulate(resultValue);
      return resultValue;
    }
    catch (ResultIterator.EofException e) {
      // Last input row. Close the input and generate the grand total row.
      input.close(true);
      inputIter = null;

      // Original code does not handle the zero-row case in all cases: it
      // will return a row of nulls in some cases, it seems. Return EOF
      // in the zero-row case. This could return the default values, as
      // done in intermediate aggregations.
      if (rowCount == 0) {
        throw Operators.eof();
      } else {
        return emitTotals();
      }
    }
  }

  private void accumulate(Result<TimeseriesResultValue> resultValue)
  {
    for (int i = 0; i < aggregatorSpecs.size(); i++) {
      final AggregatorFactory aggregatorFactory = aggregatorSpecs.get(i);
      final Object value = resultValue.getValue().getMetric(aggregatorFactory.getName());
      if (grandTotals[i] == null) {
        grandTotals[i] = value;
      } else {
        grandTotals[i] = aggregatorFactory.combine(grandTotals[i], value);
      }
    }
  }

  private Result<TimeseriesResultValue> emitTotals()
  {
    final Map<String, Object> totalsMap = new HashMap<>();
    for (int i = 0; i < aggregatorSpecs.size(); i++) {
      totalsMap.put(aggregatorSpecs.get(i).getName(), grandTotals[i]);
    }

    return new Result<>(
        null,
        new TimeseriesResultValue(totalsMap)
    );
  }

  @Override
  public void close(boolean cascade)
  {
    if (inputIter != null && cascade) {
      input.close(cascade);
    }
    inputIter = null;
    OperatorProfile profile = new OperatorProfile("grand-total-agg");
    profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
    context.updateProfile(this, profile);
  }
}

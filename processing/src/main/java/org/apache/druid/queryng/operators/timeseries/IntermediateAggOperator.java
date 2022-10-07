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
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operator.IterableOperator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.ResultIterator;

import java.util.Comparator;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * Streaming aggregation operator. Assumes values are already sorted by the
 * group keys so that like keys are adjacent. (Makes no actual assumption about
 * ordering other than adjacency.) Aggregates rows based on the aggregators
 * provided. Handles only aggregations: does not handle pass-through group
 * keys. Handles special cases such as no rows (can return a 0-like value for
 * each aggregation in this case.)
 *
 * @see {@link org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest#mergeResults
 * TimeseriesQueryQueryToolChest.mergeResults}
 * @see {@link org.apache.druid.query.ResultMergeQueryRunner ResultMergeQueryRunner}
 */
public class IntermediateAggOperator implements IterableOperator<Result<TimeseriesResultValue>>
{
  private final FragmentContext context;
  private final Operator<Result<TimeseriesResultValue>> input;
  private final Comparator<Result<TimeseriesResultValue>> resultComparator;
  private final BinaryOperator<Result<TimeseriesResultValue>> mergeFn;
  private final Supplier<Result<TimeseriesResultValue>> emptyTotalsProducer;
  private ResultIterator<Result<TimeseriesResultValue>> inputIter;
  private Result<TimeseriesResultValue> lookAheadRow;
  private int rowCount;
  private int groupCount;

  public IntermediateAggOperator(
      FragmentContext fragmentContext,
      Operator<Result<TimeseriesResultValue>> inputOp,
      Comparator<Result<TimeseriesResultValue>> resultComparator,
      BinaryOperator<Result<TimeseriesResultValue>> mergeFn,
      Supplier<Result<TimeseriesResultValue>> emptyTotalsProducer
  )
  {
    this.context = fragmentContext;
    this.input = inputOp;
    this.resultComparator = resultComparator;
    this.mergeFn = mergeFn;
    this.emptyTotalsProducer = emptyTotalsProducer;
    context.register(this);
    context.registerChild(this, inputOp);
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
      // EOF in previous group.
      throw Operators.eof();
    }

    // Start a group using the first row, or the lookahead row from the
    // previous group.
    Result<TimeseriesResultValue> row;
    Result<TimeseriesResultValue> merged;
    try {
      if (lookAheadRow == null) {
        row = inputIter.next();
        rowCount++;
      } else {
        row = lookAheadRow;
      }
      merged = mergeFn.apply(row, null);
    }
    catch (ResultIterator.EofException e) {
      // EOF on first row for the first group. Will not happen in the
      // Broker as the Historicals handle this case.
      input.close(true);
      inputIter = null;
      if (emptyTotalsProducer == null) {
        throw Operators.eof();
      } else {
        return emptyTotalsProducer.get();
      }
    }
    groupCount++;

    // Merge the other rows for the current group.
    while (true) {
      Result<TimeseriesResultValue> prevRow = row;
      try {
        row = inputIter.next();
        rowCount++;
      }
      catch (ResultIterator.EofException e) {
        // EOF while looking for the end of a group.
        input.close(true);
        inputIter = null;
        return merged;
      }
      if (resultComparator.compare(prevRow, row) != 0) {
        // New group.
        lookAheadRow = row;
        return merged;
      }

      // Another row for the same group.
      merged = mergeFn.apply(merged, row);
    }
  }

  @Override
  public void close(boolean cascade)
  {
    if (inputIter != null && cascade) {
      input.close(cascade);
    }
    inputIter = null;
    OperatorProfile profile = new OperatorProfile("intermediate-agg");
    profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
    profile.add("group-count", groupCount);
    context.updateProfile(this, profile);
  }
}

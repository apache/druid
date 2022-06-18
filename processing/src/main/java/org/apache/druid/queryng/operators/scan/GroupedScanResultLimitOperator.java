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

package org.apache.druid.queryng.operators.scan;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.MappingOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;

import java.util.List;

/**
 * Limit scan query results when each batch has multiple rows.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryLimitRowIterator}
 */
public class GroupedScanResultLimitOperator extends MappingOperator<ScanResultValue, ScanResultValue>
{
  private final long limit;
  private long rowCount;

  @VisibleForTesting
  public GroupedScanResultLimitOperator(
      FragmentContext context,
      Operator<ScanResultValue> child,
      long limit)
  {
    super(context, child);
    this.limit = limit;
  }

  @Override
  public ScanResultValue next() throws EofException
  {
    if (rowCount >= limit) {
      // Already at limit.
      throw Operators.eof();
    }

    // With throw EofException if no more input rows.
    ScanResultValue batch = inputIter.next();
    List<?> events = (List<?>) batch.getEvents();
    if (events.size() <= limit - rowCount) {
      // Entire batch is below limit.
      rowCount += events.size();
      return batch;
    } else {
      // last batch
      // single batch length is <= rowCount.MAX_VALUE, so this should not overflow
      int numLeft = (int) (limit - rowCount);
      rowCount = limit;
      return new ScanResultValue(
          batch.getSegmentId(),
          batch.getColumns(),
          events.subList(0, numLeft));
    }
  }
}

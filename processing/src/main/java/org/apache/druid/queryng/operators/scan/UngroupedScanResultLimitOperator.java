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

import java.util.ArrayList;
import java.util.List;

/**
 * Iterates over scan query results which each batch contains one row.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryLimitRowIterator}
 */
public class UngroupedScanResultLimitOperator extends MappingOperator<ScanResultValue, ScanResultValue>
{
  private final long limit;
  private final int batchSize;
  private long rowCount;

  @VisibleForTesting
  public UngroupedScanResultLimitOperator(
      FragmentContext context,
      Operator<ScanResultValue> child,
      long limit,
      int batchSize)
  {
    super(context, child);
    this.limit = limit;
    this.batchSize = batchSize;
  }

  @Override
  public ScanResultValue next() throws EofException
  {
    if (rowCount >= limit) {
      throw Operators.eof();
    }
    // Perform single-event ScanResultValue batching at the outer level.  Each
    // scan result value from the input operator in this case will only have
    // one event so there's no need to iterate through events.
    List<Object> eventsToAdd = new ArrayList<>(batchSize);
    List<String> columns = null;
    while (eventsToAdd.size() < batchSize && rowCount < limit) {
      try {
        ScanResultValue srv = inputIter.next();
        if (columns == null) {
          columns = srv.getColumns();
        }
        eventsToAdd.add(srv.getRows().get(0));
        rowCount++;
      }
      catch (EofException e) {
        if (eventsToAdd.isEmpty()) {
          throw Operators.eof();
        }
        // We'll report EOF on the next call.
        break;
      }
    }
    return new ScanResultValue(null, columns, eventsToAdd);
  }
}

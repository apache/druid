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

import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.MappingOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;

import java.util.ArrayList;
import java.util.List;

/**
 * Pack a set of individual scan results into a batch up to the
 * given size.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryLimitRowIterator}
 */
public class ScanRowToBatchOperator extends MappingOperator<ScanResultValue, ScanResultValue>
{
  private final int batchSize;

  public ScanRowToBatchOperator(
      FragmentContext context,
      Operator<ScanResultValue> child,
      int batchSize)
  {
    super(context, child);
    this.batchSize = batchSize;
  }

  @Override
  public ScanResultValue next() throws EofException
  {
    List<Object> eventsToAdd = new ArrayList<>(batchSize);
    List<String> columns = null;
    while (eventsToAdd.size() < batchSize) {
      try {
        ScanResultValue srv = inputIter.next();
        if (columns == null) {
          columns = srv.getColumns();
        }
        eventsToAdd.add(srv.getRows().get(0));
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

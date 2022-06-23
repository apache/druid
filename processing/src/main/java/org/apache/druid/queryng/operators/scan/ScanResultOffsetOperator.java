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

import java.util.List;

/**
 * Offset that skips a given number of rows on top of a skips ScanQuery. It is
 * used to implement the "offset" feature.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryOffsetSequence}
 */
public class ScanResultOffsetOperator extends MappingOperator<ScanResultValue, ScanResultValue>
{
  private final long offset;
  private long rowCount;

  public ScanResultOffsetOperator(
      FragmentContext context,
      Operator<ScanResultValue> input,
      long offset)
  {
    super(context, input);
    this.offset = offset;
  }

  @Override
  public ScanResultValue next() throws EofException
  {
    if (rowCount == 0) {
      return skip();
    } else {
      return inputIter.next();
    }
  }

  private ScanResultValue skip() throws EofException
  {
    while (true) {
      ScanResultValue batch = inputIter.next();
      final List<Object> rows = batch.getRows();
      final int eventCount = rows.size();
      final long toSkip = offset - rowCount;
      if (toSkip >= eventCount) {
        rowCount += eventCount;
        continue;
      }
      rowCount += eventCount - toSkip;
      return new ScanResultValue(
          batch.getSegmentId(),
          batch.getColumns(),
          rows.subList((int) toSkip, eventCount));
    }
  }
}

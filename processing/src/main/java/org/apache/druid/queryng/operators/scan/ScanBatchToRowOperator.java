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

import java.util.Iterator;
import java.util.List;

/**
 * Converts an input operator which returns scan query "batches" to individual map records.
 * The record type is assumed to be one of the valid
 * {@link org.apache.druid.query.scan.ScanQuery.ResultFormat
 * ResultFormat} types.
 */
public class ScanBatchToRowOperator<T> extends MappingOperator<ScanResultValue, T>
{
  private Iterator<T> batchIter;

  public ScanBatchToRowOperator(FragmentContext context, Operator<ScanResultValue> input)
  {
    super(context, input);
  }

  @Override
  @SuppressWarnings("unchecked")
  public T next() throws EofException
  {
    while (true) {
      if (batchIter == null) {
        batchIter = ((List<T>) (inputIter.next().getRows())).iterator();
      }
      if (batchIter.hasNext()) {
        return batchIter.next();
      }
      batchIter = null;
    }
  }

  @Override
  public void close(boolean cascade)
  {
    batchIter = null;
    super.close(cascade);
  }
}

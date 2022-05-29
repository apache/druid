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
import org.apache.druid.queryng.operators.MappingOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.ResultIterator;

import java.util.List;
import java.util.Map;

public class ToArrayOperator extends MappingOperator<Result<TimeseriesResultValue>, Object[]>
{
  private final List<String> fields;
  private int rowCount;

  public ToArrayOperator(
      FragmentContext context,
      Operator<Result<TimeseriesResultValue>> input,
      final List<String> fields
  )
  {
    super(context, input);
    this.fields = fields;
  }

  @Override
  public Object[] next() throws ResultIterator.EofException
  {
    Result<TimeseriesResultValue> result = inputIter.next();
    rowCount++;
    final Object[] retVal = new Object[fields.size()];

    // Position 0 is always __time.
    retVal[0] = result.getTimestamp().getMillis();

    // Add other fields.
    final Map<String, Object> resultMap = result.getValue().getBaseObject();
    for (int i = 1; i < fields.size(); i++) {
      retVal[i] = resultMap.get(fields.get(i));
    }

    return retVal;
  }

  @Override
  public void close(boolean cascade)
  {
    if (inputIter != null) {
      OperatorProfile profile = new OperatorProfile("timeseries-to-array");
      profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
      context.updateProfile(this, profile);
    }
    super.close(cascade);
  }
}

/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.select;

import com.metamx.common.guava.nary.BinaryFn;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Result;
import org.joda.time.DateTime;

/**
 */
public class SelectBinaryFn
    implements BinaryFn<Result<SelectResultValue>, Result<SelectResultValue>, Result<SelectResultValue>>
{
  private final QueryGranularity gran;
  private final PagingSpec pagingSpec;
  private final boolean descending;

  public SelectBinaryFn(
      QueryGranularity granularity,
      PagingSpec pagingSpec,
      boolean descending
  )
  {
    this.gran = granularity;
    this.pagingSpec = pagingSpec;
    this.descending = descending;
  }

  @Override
  public Result<SelectResultValue> apply(
      Result<SelectResultValue> arg1, Result<SelectResultValue> arg2
  )
  {
    if (arg1 == null) {
      return arg2;
    }

    if (arg2 == null) {
      return arg1;
    }

    final DateTime timestamp = (gran instanceof AllGranularity)
                               ? arg1.getTimestamp()
                               : gran.toDateTime(gran.truncate(arg1.getTimestamp().getMillis()));

    SelectResultValueBuilder builder = new SelectResultValueBuilder(timestamp, pagingSpec, descending);

    SelectResultValue arg1Val = arg1.getValue();
    SelectResultValue arg2Val = arg2.getValue();

    for (EventHolder event : arg1Val) {
      builder.addEntry(event);
    }

    for (EventHolder event : arg2Val) {
      builder.addEntry(event);
    }

    return builder.build();
  }
}

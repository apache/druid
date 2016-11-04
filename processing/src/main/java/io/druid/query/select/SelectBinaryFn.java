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

import com.google.common.collect.Sets;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.Result;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Set;

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

    final List<EventHolder> arg1Val = arg1.getValue().getEvents();
    final List<EventHolder> arg2Val = arg2.getValue().getEvents();

    if (arg1Val == null || arg1Val.isEmpty()) {
      return arg2;
    }

    if (arg2Val == null || arg2Val.isEmpty()) {
      return arg1;
    }

    final DateTime timestamp = (gran instanceof AllGranularity)
                               ? arg1.getTimestamp()
                               : gran.toDateTime(gran.truncate(arg1.getTimestamp().getMillis()));

    SelectResultValueBuilder builder = new SelectResultValueBuilder.MergeBuilder(timestamp, pagingSpec, descending);

    builder.addDimensions(mergeColumns(arg1.getValue().getDimensions(), arg2.getValue().getDimensions()));
    builder.addMetrics(mergeColumns(arg1.getValue().getMetrics(), arg2.getValue().getMetrics()));

    for (EventHolder event : arg1Val) {
      builder.addEntry(event);
    }

    for (EventHolder event : arg2Val) {
      builder.addEntry(event);
    }

    return builder.build();
  }

  private Set<String> mergeColumns(final Set<String> arg1, final Set<String> arg2)
  {
    if (arg1.isEmpty()) {
      return arg2;
    }

    if (arg2.isEmpty()) {
      return arg1;
    }

    if (arg1.equals(arg2)) {
      return arg1;
    }

    return Sets.union(arg1, arg2);
  }
}

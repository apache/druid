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

package org.apache.druid.segment.transform;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.segment.incremental.InputRowFilterResult;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Marker row used internally when a row was rejected before downstream ingestion filters can classify it.
 */
public final class FilteredInputRow implements InputRow
{
  public static final FilteredInputRow CUSTOM_FILTER = new FilteredInputRow(InputRowFilterResult.CUSTOM_FILTER);

  private final InputRowFilterResult filterResult;

  private FilteredInputRow(final InputRowFilterResult filterResult)
  {
    this.filterResult = filterResult;
  }

  public InputRowFilterResult getFilterResult()
  {
    return filterResult;
  }

  @Override
  public List<String> getDimensions()
  {
    throw unsupportedOperationException();
  }

  @Override
  public long getTimestampFromEpoch()
  {
    throw unsupportedOperationException();
  }

  @Override
  public DateTime getTimestamp()
  {
    throw unsupportedOperationException();
  }

  @Override
  public List<String> getDimension(final String dimension)
  {
    throw unsupportedOperationException();
  }

  @Nullable
  @Override
  public Object getRaw(final String dimension)
  {
    throw unsupportedOperationException();
  }

  @Nullable
  @Override
  public Number getMetric(final String metric)
  {
    throw unsupportedOperationException();
  }

  @Override
  public int compareTo(final Row o)
  {
    if (o instanceof FilteredInputRow) {
      return filterResult.compareTo(((FilteredInputRow) o).filterResult);
    }
    throw unsupportedOperationException();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FilteredInputRow that = (FilteredInputRow) o;
    return filterResult == that.filterResult;
  }

  @Override
  public int hashCode()
  {
    return filterResult.hashCode();
  }

  private UnsupportedOperationException unsupportedOperationException()
  {
    return new UnsupportedOperationException("Filtered input rows do not carry row data");
  }
}

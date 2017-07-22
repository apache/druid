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

package io.druid.indexer.hadoop;

import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import org.joda.time.DateTime;

import java.util.List;

/**
 * SegmentInputRow serves as a marker that these InputRow instances have already been combined
 * and they contain the columns as they show up in the segment after ingestion, not what you would see in raw
 * data.
 * It must only be used to represent such InputRows.
 */
public class SegmentInputRow implements InputRow
{
  private final InputRow delegate;

  public SegmentInputRow(InputRow delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public List<String> getDimensions()
  {
    return delegate.getDimensions();
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return delegate.getTimestampFromEpoch();
  }

  @Override
  public DateTime getTimestamp()
  {
    return delegate.getTimestamp();
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    return delegate.getDimension(dimension);
  }

  @Override
  public Object getRaw(String dimension)
  {
    return delegate.getRaw(dimension);
  }

  @Override
  public float getFloatMetric(String metric)
  {
    return delegate.getFloatMetric(metric);
  }

  @Override
  public long getLongMetric(String metric)
  {
    return delegate.getLongMetric(metric);
  }

  @Override
  public double getDoubleMetric(String metric)
  {
    return delegate.getDoubleMetric(metric);
  }

  @Override
  public int compareTo(Row row)
  {
    return delegate.compareTo(row);
  }

  public InputRow getDelegate()
  {
    return delegate;
  }

  @Override
  public String toString()
  {
    return "SegmentInputRow{" +
           "delegate=" + delegate +
           '}';
  }
}
